package ai.fma.mpi_yarn;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import ai.fma.mpi_yarn.launcher.LauncherProcess;
import ai.fma.mpi_yarn.launcher.OrteLauncherProcess;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMaster {
	private String hostname;
	private int responseId = 0;

	{
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	private static Logger logger = LoggerFactory.getLogger(ApplicationMaster.class);

	public static void clientPrint(FSDataOutputStream outputStream, String mesg) throws IOException {
		System.out.print(mesg);
		outputStream.writeBytes(mesg);
		outputStream.hsync();
	}

	public static void clientPrintln(FSDataOutputStream outputStream, String mesg) throws IOException {
		System.out.println(mesg);
		outputStream.writeBytes(mesg + "\n");
		outputStream.hsync();
	}

	public static void main(String[] args) throws Exception {
		logger.debug("Application Master Start");

		ApplicationMaster appMaster = new ApplicationMaster();

		MyConf myConf = MyConf.deserialize(System.getenv(MyConf.MY_CONF_SERIALIZED));
		FileSystem dfs = myConf.getFileSystem();
		logger.debug("  output path = " + myConf.getOutputPath());
		FSDataOutputStream outputStream = dfs.create(new Path(myConf.getOutputPath()));

		AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
		rmClient.init(myConf.getYarnConfiguration());
		rmClient.start();

		NMClient nmClient = NMClient.createNMClient();
		nmClient.init(myConf.getYarnConfiguration());
		nmClient.start();

		// Register with ResourceManager
		rmClient.registerApplicationMaster("", 0, "");
		logger.debug("Registered Application Master");

		ArrayList<Container> containers = new ArrayList<Container>();
		HashMap<String, Container> deamonHostContainer = new HashMap<String, Container>();
		Resource deamonResource = getDeamonResourceRequired(myConf);
		int priority = 0;

		// AM is an implicit deamon in ORTE
		HashSet<String> exceptHosts = new HashSet<String>();
		if (myConf.getLauncherType() == MyConf.LAUNCHER_TYPE_ORTE) {
			exceptHosts.add(appMaster.hostname);
		}
		appMaster.acquireContainersDistinctHost(rmClient, myConf.getNumDeamons(), deamonResource, priority, exceptHosts, deamonHostContainer);

		int num_nodes = myConf.getNumNodes();
		int ppn = myConf.getNumProcsPerNode();

		logger.debug("Requesting resources");
		logger.debug("amHostName = " + appMaster.hostname);
		logger.debug("num_nodes = " + num_nodes);
		logger.debug("num_deamons = " + myConf.getNumDeamons());
		logger.debug("nprocs_per_node = " + ppn);

		clientPrintln(outputStream, "acquired container list: [" + String.join(",", deamonHostContainer.keySet()) + "]");

		// specify local resources in distributed cache
		Map<String, LocalResource> localResources = myConf.createDeamonLocalResources();

		// setting required environment variables
		Map<String, String> containerEnv = new HashMap<String, String>();
		setDeamonContainerEnv(myConf, containerEnv);

//		ArrayList<Container> containerSequence = new ArrayList<Container>();
//		StringBuilder hostSb = new StringBuilder();
//		{
//			boolean first = true;
//			for (String host : hostContainers.keySet()) {
//				for (Container container : hostContainers.get(host)) {
//					if (first) {
//						first = false;
//					} else {
//						hostSb.append(",");
//					}
//					hostSb.append(host);
//					containerSequence.add(container);
//				}
//			}
//		}

		// run the launcher process
		LauncherProcess launcher_process;
		if (myConf.getLauncherType() == MyConf.LAUNCHER_TYPE_HYDRA) {
			throw new NotImplementedException("Not implemented");
			// launcher_process = new HydraLauncherProcess(myConf, deamonHostContainer.keySet(), ppn);
		} else if (myConf.getLauncherType() == MyConf.LAUNCHER_TYPE_ORTE) {
			launcher_process = new OrteLauncherProcess(myConf, deamonHostContainer.keySet(), ppn);
		} else {
			throw new IllegalArgumentException("Unknown launcher type");
		}

		Map<String, String> deamonHostCommand = launcher_process.getDeamonHostCommand(myConf);

		// submit the deamons
		for(String host : deamonHostContainer.keySet()) {
			Container container = deamonHostContainer.get(host);
			String deamon_commands = deamonHostCommand.get(host);
			submitToContainer(nmClient, deamon_commands, localResources, containerEnv, container);
		}

		// polling the stdout of launcher process into specified output stream
		appMaster.waitForCompletion(rmClient, deamonHostContainer.size(), launcher_process, outputStream);

		// Un-register with ResourceManager
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}

	// wait for the completion of launcher process and write its stdout, stderr into specified outputStream
	private void waitForCompletion(AMRMClient<ContainerRequest> rmClient, int numContainers, LauncherProcess launcher_process, FSDataOutputStream outputStream) throws IOException, YarnException, InterruptedException {
		logger.debug("Waiting for completion of the launcher and all the containers");
		InputStream mpirunIstream = launcher_process.getInputStream();
		InputStream mpirunEstream = launcher_process.getErrorStream();
		// Scanner mpirunScanner = new Scanner(mpirunIstream);
		// Scanner mpirunEscanner = new Scanner(mpirunEstream);

		int bufferBytes = 4096;
		byte[] buffer = new byte[bufferBytes];
		int completedContainers = 0;
		boolean iStreamClosed = false;
		boolean eStreamClosed = false;
		while (completedContainers < numContainers) {
			AllocateResponse response = rmClient.allocate(responseId++);
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				if (status.getExitStatus() != 0) {
					clientPrintln(outputStream, "Completed container with non-zero exit code " + status.getContainerId()
							+ " with exit code " + status.getExitStatus());
				} else {
					logger.debug("Completed container " + status.getContainerId() + " with exit code "
							+ status.getExitStatus());
				}
			}
			if (mpirunIstream.available() > 0) {
				int bytes = mpirunIstream.read(buffer, 0, bufferBytes);
				if (bytes == -1) {
					iStreamClosed = true;
				} else {
					String next = new String(buffer, 0, bytes);
					clientPrint(outputStream, next);
				}
			}
			if (mpirunEstream.available() > 0) {
				int bytes = mpirunEstream.read(buffer, 0, bufferBytes);
				if (bytes == -1) {
					eStreamClosed = true;
				} else {
					String next = new String(buffer, 0, bytes);
					clientPrint(outputStream, next);
				}
			}
			Thread.sleep(MyConf.SLEEP_INTERVAL_MS);
		}
		logger.debug("Draining the stdout of launcher");
		{
			int bytes = 0;
			while (!iStreamClosed) {
				bytes = mpirunIstream.read(buffer, 0, bufferBytes);
				if (bytes == -1) {
					iStreamClosed = true;
				} else {
					String next = new String(buffer, 0, bytes);
					clientPrint(outputStream, next);
				}
			}
		}
		logger.debug("Draining the stderr of launcher");
		{
			int bytes = 0;
			while (!eStreamClosed) {
				bytes = mpirunEstream.read(buffer, 0, bufferBytes);
				if (bytes == -1) {
					eStreamClosed = true;
				} else {
					String next = new String(buffer, 0, bytes);
					clientPrint(outputStream, next);
				}
			}
		}
	}

	// submit container_cmd to the container
	private static void submitToContainer(NMClient nmClient, String container_cmd, Map<String, LocalResource> localResources, Map<String, String> containerEnv, Container container) throws IOException, YarnException {
		logger.debug(String.format("Launching container %s (%s)", container.getId(), container_cmd));
		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setLocalResources(localResources);
		ArrayList<String> commands = new ArrayList<String>();
		commands.add(container_cmd + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
				+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
		// commands.add("echo ContainerFinished!");
		ctx.setCommands(commands);
		ctx.setEnvironment(containerEnv);
		nmClient.startContainer(container, ctx);
	}

	// acquire specified number of containers with specified resource and priority to deamonHostContainer with distinct hosts and except exceptHosts
	private void acquireContainersDistinctHost(AMRMClient<ContainerRequest> rmClient, int numContainers, Resource resource, int priority_num, Set<String> exceptHosts, HashMap<String, Container> deamonHostContainer) throws IOException, YarnException {
		// Priority for worker containers - priorities are intra-application
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(priority_num);
		while (deamonHostContainer.size() < numContainers) {
			ContainerRequest containerReq = new ContainerRequest(resource, null, null, priority);
			rmClient.addContainerRequest(containerReq);
			while (true) {
				AllocateResponse response = rmClient.allocate(responseId++);
				if (response.getAllocatedContainers().size() > 0) {
					Container container = response.getAllocatedContainers().get(0);
					NodeId nodeId = container.getNodeId();
					String host = nodeId.getHost();
					if (!deamonHostContainer.containsKey(host) && !exceptHosts.contains(host)) {
						deamonHostContainer.put(host, container);
						logger.debug("acquired a container from host " + host);
						break;
					} else {
						rmClient.releaseAssignedContainer(container.getId());
						logger.debug("ignored a duplicate container from host " + host);
					}
				}
			}
		}
	}

	// get required Resource from configuration
	private static Resource getDeamonResourceRequired(MyConf myConf) {
		Resource resource = Records.newRecord(Resource.class);
		int vcores_per_node = myConf.getVCoresPerNode();
		long mb_memory_per_node = myConf.getMemoryMbPerNode();
		long mb_nvdimm_per_node = myConf.getNvdimmMbPerNode();
		logger.debug("Deamon Container Resource");
		logger.debug("  vcores_per_node = " + vcores_per_node);
		logger.debug("  mb_memory_per_node = " + mb_memory_per_node);
		logger.debug("  mb_nvdimm_per_node = " + mb_nvdimm_per_node);
		resource.setVirtualCores(vcores_per_node);
		resource.setMemorySize(mb_memory_per_node);
		resource.setResourceValue(MyConf.NVDIMM_RESOURCE_NAME, mb_nvdimm_per_node);
		return resource;
	}

	// set environment variable for daemon
	//   1. User required
	//   2. LD_LIBRARY_PATH
	private static void setDeamonContainerEnv(MyConf myConf, Map<String, String> containerEnv) {
		// set required variable
		for (String envName : System.getenv().keySet()) {
			if (myConf.getEnvKeys().contains(envName)) {
				containerEnv.put(envName, System.getenv(envName));
			}
		}
		// set LD_LIBRARY_PATH for shared objects
		String ldLibraryPath = containerEnv.get("LD_LIBRARY_PATH");
		if (ldLibraryPath == null) {
			ldLibraryPath = ".";
		} else {
			ldLibraryPath = ".:" + ldLibraryPath;
		}
		containerEnv.put("LD_LIBRARY_PATH", ldLibraryPath);
		logger.debug("[Environment Variable for Launcher and Deamons]");
		for (String key : containerEnv.keySet()) {
			logger.debug(key + " = " + containerEnv.get(key));
		}
	}

}
