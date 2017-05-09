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
		Path hdfsPrefix = new Path(myConf.getHdfsPrefix());
		int responseId = 0;

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
		}

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		{
			Path executablePath = Path.mergePaths(hdfsPrefix, new Path(myConf.getExecutableName()));
			LocalResource executableResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, executablePath, executableResource);
			localResources.put(executablePath.getName(), executableResource);
		}
		{
			Path pmiProxyPath = Path.mergePaths(hdfsPrefix, new Path(MyConf.PMI_PROXY));
			LocalResource pmiProxyResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, pmiProxyPath, pmiProxyResource);
			localResources.put(pmiProxyPath.getName(), pmiProxyResource);
		}
		for (String sofile : myConf.getSharedObjectPathList()) {
			Path src = new Path(sofile);
			Path target = Path.mergePaths(hdfsPrefix, new Path("sofiles/" + src.getName()));
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

		System.out.println("append output into " + myConf.getOutputPath());
		Path outputPath = new Path(myConf.getOutputPath());
		FSDataOutputStream outputStream = dfs.create(outputPath);

		InputStream mpirunIstream;
		InputStream mpirunEstream;
		Scanner mpirunScanner;
		Scanner mpirunEscanner;
		{
			String cmd = MessageFormat.format("./{0} -launcher manual -ppn 1 -hosts {1} {2} {3}", MyConf.MPIEXEC, hostSb.toString(),
					myConf.getExecutableName(), myConf.getExecutableArgs());
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
				ctx.setCommands(
						Collections.singletonList(container_cmd + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
								+ "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
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
		int completedContainers = 0;
		while (completedContainers < containers.size()) {
			AllocateResponse response = rmClient.allocate(responseId++);
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				System.out.println(
						"Completed container " + status.getContainerId() + " with exit code " + status.getExitStatus());
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
	}
}
