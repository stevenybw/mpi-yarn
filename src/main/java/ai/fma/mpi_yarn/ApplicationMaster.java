package ai.fma.mpi_yarn;

import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {

	public static void main(String[] args) throws Exception {
		System.out.println("AM Start");

		// Initialize clients to ResourceManager and NodeManagers
		Configuration conf = new YarnConfiguration();

		FileSystem dfs = FileSystem.get(conf);
		MyConf myConf = MyConf.deserialize(System.getenv(MyConf.EnvName));
		String hdfsPrefix = myConf.getHdfsPrefix();
		int responseId = 0;

		System.out.println("append output into " + myConf.getOutputPath());
		Path outputPath = new Path(myConf.getOutputPath());
		FSDataOutputStream outputStream = dfs.create(outputPath);

		AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
		rmClient.init(conf);
		rmClient.start();

		NMClient nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();

		// Register with ResourceManager
		System.out.println("registerApplicationMaster 0");
		rmClient.registerApplicationMaster("", 0, "");
		System.out.println("registerApplicationMaster 1");

		// Priority for worker containers - priorities are intra-application
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);

		ArrayList<Container> containers = new ArrayList<Container>();
		HashMap<String, ArrayList<Container>> hostContainers = new HashMap<String, ArrayList<Container>>();

		if (myConf.getLocalityType() == LocalityType.NONE) {
			int n = myConf.getNumProcs();
			System.out.println("request " + String.valueOf(n) + " container; container memory = "
					+ String.valueOf(myConf.getContainerMemoryMb()) + "MB");
			// Resource requirements for worker containers
			Resource capability = Records.newRecord(Resource.class);
			capability.setMemory(myConf.getContainerMemoryMb());
			capability.setVirtualCores(1);

			// Make container requests to ResourceManager
			for (int i = 0; i < n; ++i) {
				ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
				System.out.println("Making res-req " + i);
				rmClient.addContainerRequest(containerAsk);
			}

			// Obtain allocated containers
			while (containers.size() < n) {
				AllocateResponse response = rmClient.allocate(responseId++);
				for (Container container : response.getAllocatedContainers()) {
					NodeId nodeId = container.getNodeId();
					String host = nodeId.getHost();
					System.out.println("Acquired container " + container.getId() + " at host " + host);
					containers.add(container);
					if (!hostContainers.containsKey(host)) {
						hostContainers.put(host, new ArrayList<Container>());
					}
					hostContainers.get(host).add(container);
				}
				Thread.sleep(100);
			}
		} else if (myConf.getLocalityType() == LocalityType.GROUP) {
			int N = myConf.getNumNodes();
			int ppn = myConf.getNumProcsPerNode();
			System.out.println("request " + N + " groups; each group has " + ppn + " containers; container memory = "
					+ String.valueOf(myConf.getContainerMemoryMb()));
			// Resource requirement is the same as above
			Resource capability = Records.newRecord(Resource.class);
			capability.setMemory(myConf.getContainerMemoryMb());
			capability.setVirtualCores(1);

			// acquired group is a group which has more than ppn nodes
			int numAcquiredGroup = 0;

			// request one container at a time, until we have enough groups
			while (numAcquiredGroup < N) {
				ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
				rmClient.addContainerRequest(containerAsk);

				// wait for one container
				while (true) {
					AllocateResponse response = rmClient.allocate(responseId++);
					if (response.getAllocatedContainers().size() > 0) {
						Container container = response.getAllocatedContainers().get(0);
						NodeId nodeId = container.getNodeId();
						String host = nodeId.getHost();
						System.out.println("Acquired container " + container.getId() + " at host " + host);
						if (!hostContainers.containsKey(host)) {
							hostContainers.put(host, new ArrayList<Container>());
						}
						hostContainers.get(host).add(container);
						if (hostContainers.get(host).size() == ppn) {
							numAcquiredGroup++;
						}
						break;
					}
				}
			}

			{
				ArrayList<Container> redundantContainers = new ArrayList<Container>();

				// find the redundant containers & update containers
				for (String host : hostContainers.keySet()) {
					ArrayList<Container> Cs = hostContainers.get(host);
					if (Cs.size() < ppn) {
						redundantContainers.addAll(Cs);
						hostContainers.remove(host);
					} else {
						for (int i = 0; i < (Cs.size() - ppn); i++) {
							redundantContainers.add(Cs.remove(0));
						}
						containers.addAll(Cs);
					}
				}

				// release the redundant containers
				for (Container container : redundantContainers) {
					System.out.println("Releasing redundant container " + container.getId());
					rmClient.releaseAssignedContainer(container.getId());
				}
			}
		}

		System.out.println("resource complete: ");
		for (String host : hostContainers.keySet()) {
			System.out.println("   " + host + ":" + hostContainers.get(host).size());
		}

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		{
			Path executablePath = new Path(hdfsPrefix + "/" + myConf.getExecutableName());
			LocalResource executableResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, executablePath, executableResource);
			localResources.put(executablePath.getName(), executableResource);
		}
		{
			Path pmiProxyPath = new Path(hdfsPrefix + "/" + MyConf.PMI_PROXY);
			LocalResource pmiProxyResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, pmiProxyPath, pmiProxyResource);
			localResources.put(pmiProxyPath.getName(), pmiProxyResource);
		}
		for (String sofile : myConf.getSharedObjectPathList()) {
			Path src = new Path(sofile);
			Path target = new Path(hdfsPrefix + "/sofiles/" + src.getName());
			LocalResource soResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, target, soResource);
			localResources.put(target.getName(), soResource);
		}
		HashSet<String> envList = myConf.getEnvList();
		Map<String, String> containerEnv = new HashMap<String, String>();
		for (String envName : System.getenv().keySet()) {
			if (envList.contains(envName)) {
				containerEnv.put(envName, System.getenv(envName));
			}
		}
		String ldLibraryPath = containerEnv.get("LD_LIBRARY_PATH");
		if (ldLibraryPath == null) {
			ldLibraryPath = "./sofiles";
		} else {
			ldLibraryPath = "./sofiles:" + ldLibraryPath;
		}
		containerEnv.put("LD_LIBRARY_PATH", ldLibraryPath);
		System.out.println("=== Environment ===");
		System.out.println(containerEnv);
		System.out.println("===================");

		ArrayList<Container> containerSequence = new ArrayList<Container>();
		StringBuilder hostSb = new StringBuilder();
		{
			boolean first = true;
			for (String host : hostContainers.keySet()) {
				for (Container container : hostContainers.get(host)) {
					if (first) {
						first = false;
					} else {
						hostSb.append(",");
					}
					hostSb.append(host);
					containerSequence.add(container);
				}
			}
		}

		InputStream mpirunIstream;
		InputStream mpirunEstream;
		Scanner mpirunScanner;
		Scanner mpirunEscanner;
		{
			String cmd = MessageFormat.format(
					"./{0} -launcher manual -ppn 1 -hosts {1} -wdir /tmp {2} {3}", MyConf.MPIEXEC,
					hostSb.toString(), myConf.getExecutableName(), myConf.getExecutableArgs());
			System.out.println("invoke " + cmd);
			ProcessBuilder pb = new ProcessBuilder(cmd.split("\\s"));
			Process p = pb.start();
			mpirunIstream = p.getInputStream();
			mpirunEstream = p.getErrorStream();
			mpirunScanner = new Scanner(mpirunIstream);
			mpirunEscanner = new Scanner(mpirunEstream);
			for (Container container : containerSequence) {
				String line = mpirunScanner.nextLine();
				// HYDRA_LAUNCH: /Users/ybw/local/mpich-3.2/bin/hydra_pmi_proxy
				// --control-port 172.23.100.68:58247 --rmk user --launcher
				// manual --demux poll --pgid 0 --retries 10 --usize -2
				// --proxy-id 0
				String[] sp = line.split(" ");
				String[] sub_sp = Arrays.copyOfRange(sp, 2, sp.length);
				String container_cmd = "./" + MyConf.PMI_PROXY + " " + StringUtils.join(sub_sp, " ");
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				ctx.setLocalResources(localResources);
				ArrayList<String> commands = new ArrayList<String>();
				commands.add(container_cmd + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
				// commands.add("echo ContainerFinished!");
				ctx.setCommands(commands);

				nmClient.startContainer(container, ctx);
				System.out.println("Launching container " + container.getId() + " with cmd " + container_cmd);
			}
			{
				String nextLine = mpirunScanner.nextLine();
				if (!nextLine.startsWith("HYDRA_LAUNCH_END")) {
					throw new RuntimeException("Not Start With HYDRA_LAUNCH_END, but " + nextLine);
				}
			}
		}

		// Wait for containers
		boolean errorLaunchingContainer = false;
		int completedContainers = 0;
		while (completedContainers < containers.size()) {
			AllocateResponse response = rmClient.allocate(responseId++);
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				System.out.println(
						"Completed container " + status.getContainerId() + " with exit code " + status.getExitStatus());
				if (status.getExitStatus() != 0) {
					errorLaunchingContainer = true;
				}
			}
			if (mpirunIstream.available() > 0) {
				String nextLine = mpirunScanner.nextLine();
				System.out.println(nextLine);
				outputStream.writeBytes(nextLine + "\n");
				outputStream.hsync();
			}
			if (mpirunEstream.available() > 0) {
				String nextLine = mpirunEscanner.nextLine();
				System.out.println("[stderr] " + nextLine);
				outputStream.writeBytes("[stderr] " + nextLine + "\n");
				outputStream.hsync();
			}
			Thread.sleep(100);
		}
		if (!errorLaunchingContainer) {
			while (mpirunScanner.hasNext()) {
				String nextLine = mpirunScanner.nextLine();
				System.out.println(nextLine);
				outputStream.writeBytes(nextLine + "\n");
				outputStream.hsync();
			}
			while (mpirunEscanner.hasNext()) {
				String nextLine = mpirunEscanner.nextLine();
				System.out.println("[stderr] " + nextLine);
				outputStream.writeBytes("[stderr] " + nextLine + "\n");
				outputStream.hsync();
			}
			// Un-register with ResourceManager
			rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
		} else {
			rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "failed due to one of the container launch returns non-zero", "");
		}
	}
}
