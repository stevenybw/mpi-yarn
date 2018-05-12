
package ai.fma.mpi_yarn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyConf implements Serializable {
	private static final String LOCALIZED_CONF_DIR = "__mpi_yarn_conf__";
	private static final Logger logger = LoggerFactory.getLogger(MyConf.class);
	public static final String SPLIT_SPACE = "\\s+";

	transient public YarnConfiguration yarnConfiguration = new YarnConfiguration();
	transient private FileSystem fileSystem = FileSystem.get(yarnConfiguration);

	public static final int LAUNCHER_TYPE_HYDRA = 1;
	public static final int LAUNCHER_TYPE_ORTE = 2;
	public static final long SLEEP_INTERVAL_MS = 200;
	public static String testSS = "mytest.sh";
	public static String NVDIMM_RESOURCE_NAME = "nvdimm";

	// TODO
	private int launcherType;
	private String proxyClientPath;
	private String launcherClientPath;

	// Memory required by AM alone
	private long amMemoryMb;

	public static String serialize(MyConf conf) {
		byte[] data = SerializationUtils.serialize(conf);
		return Base64.encodeBase64String(data);
	}

	public static MyConf deserialize(String base64String) throws IOException {
		byte[] data = Base64.decodeBase64(base64String);
		MyConf myConf = (MyConf) SerializationUtils.deserialize(data);
		myConf.yarnConfiguration = new YarnConfiguration();
		myConf.fileSystem = FileSystem.get(myConf.yarnConfiguration);
		return myConf;
	}

	public String getQueueName() {
		return queueName;
	}

	/**
	 * Return memory required by the java part of Application Master in MB
	 */
	public long getAMJavaMemoryMb() { return amMemoryMb; }

	public Date getNow() { return now; }
	public String getHdfsPrefix() {
		return hdfsPrefix;
	}
	public String getExecutableArgs() {
		return executableArgs;
	}
	public String getOutputPath() {
		return outputPath;
	}
	public int getNumNodes() {
		return numNodes;
	}
	public int getNumProcsPerNode() {
		return procsPerNode;
	}
	public HashMap<String, String> getSharedObjects() { return sharedObjects; }
	public HashMap<String, String> getAdditionalResource() { return additionalResources; }
	public List<String> getEnvKeys() {
		return envKeys;
	}
	public List<String> getEnvValues() {
		return envValues;
	}

	private Date now;
	private String hdfsPrefix;

	private List<String> envKeys;
	private List<String> envValues;
	private String executableClientPath;
	private String executableArgs;

	// HDFS Path for program output
	private String outputPath;

	// Total number of nodes for this submission
	private int numNodes;

	// MB nvdimm for each node
	private long nvdimmMbPerNode;

	// MB memory for each node
	private long memoryMbPerNode;

	// Number of vCore for each node
	private int vCoresPerNode;

	// Number of procs to launch for each node
	private int procsPerNode;

	// Path to this jar
	private String containingJarClientPath;

	// Queue name
	private String queueName;

	// Depended shared objects: name -> hdfsPath
    private HashMap<String, String> sharedObjects;

	// Additional resources: name -> hdfsPath
	private HashMap<String, String> additionalResources;

	MyConf(String[] args) throws IOException {
		now = new Date();

		Options options = new Options();
		addOption(options, true, "a", "application", true, "Path to the MPI executable");
		addOption(options, true, "j", "jar", true, "The path to MPI-YARN jar file");
		addOption(options, true, "p", "prefix", true, "HDFS prefix");
		addOption(options, true, "lt","launcher_type", true, "Launcher type (hydra/orte)");
		addOption(options, true, "dp","deamon_path", true, "Path to the launcher deamon");
		addOption(options, true, "lp", "launcher_path", true, "Path to launcher");
		addOption(options, true, "N", "num_nodes", true, "Total number of nodes");
		addOption(options, true, "ppn", "processes_per_node", true, "Number of processes to launch per node");
		addOption(options, true, "vcpn", "vcores_per_node", true, "Number of vcores per node");
		addOption(options, true, "mbmpn", "mb_memory_per_node", true, "Memory capacity in MB per node");
		addOption(options, true, "mbnpn", "mb_nvdimm_per_node", true, "Nvdimm capacity in MB per node");
		addOption(options, true, "mbam", "mb_application_master", true, "Memory capacity for ApplicationMaster");
		addOption(options, true, "q", "queue", true, "Queue name");
		addOption(options, false, "o", "output", true, "HDFS Path For Output");
		addOption(options, false, "args", "", true, "Argument to the MPI executable");
		addOption(options, false, "envlist", "", true, "column-separated list of env-names to pass");
		addOption(options, false, "sharedlist", "", true, "column-separeted list of shared-object-paths");
		addOption(options, false, "r", "resources", true, "column-separated list of resources ${Local_File_Name}=${HDFS_Path}");
		CommandLineParser parser = new org.apache.commons.cli.PosixParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("mpi-run", options);
			System.exit(1);
			return;
		}
		// process
		executableClientPath = cmd.getOptionValue("a");
		assertPathExist(executableClientPath);
		containingJarClientPath = cmd.getOptionValue("j");
		assertPathExist(containingJarClientPath);
		queueName = getOptionValue(cmd, "q", "default");
		hdfsPrefix = cmd.getOptionValue("p");
		String launcherTypeStr = cmd.getOptionValue("lt");
		switch (launcherTypeStr.toLowerCase()) {
			case "hydra":
				launcherType = LAUNCHER_TYPE_HYDRA;
				break;
			case "orte":
				launcherType = LAUNCHER_TYPE_ORTE;
				break;
			default:
				assert (false);
				break;
		}
		proxyClientPath = cmd.getOptionValue("dp");
		assertPathExist(proxyClientPath);
		launcherClientPath = cmd.getOptionValue("lp");
		assertPathExist(launcherClientPath);
		numNodes = Integer.valueOf(cmd.getOptionValue("N"));
		procsPerNode = Integer.valueOf(cmd.getOptionValue("ppn"));
		vCoresPerNode = Integer.valueOf(cmd.getOptionValue("vcpn"));
		memoryMbPerNode = Long.valueOf(cmd.getOptionValue("mbmpn"));
		nvdimmMbPerNode = Long.valueOf(cmd.getOptionValue("mbnpn"));
		amMemoryMb = Long.valueOf(cmd.getOptionValue("mbam"));
		Calendar calendar = Calendar.getInstance();
		String defaultOutputPath = hdfsPrefix + "/output." + getExecutableName() + "." + String.valueOf(calendar.get(Calendar.YEAR)) + "_"
				+ String.valueOf(calendar.get(Calendar.MONTH)) + "_" + String.valueOf(calendar.get(Calendar.DAY_OF_MONTH)) + "_"
				+ String.valueOf(calendar.get(Calendar.HOUR_OF_DAY)) + "_" + String.valueOf(calendar.get(Calendar.MINUTE)) + "_"
				+ String.valueOf(calendar.get(Calendar.SECOND)) + ".txt";
		outputPath = getOptionValue(cmd, "o", defaultOutputPath);
		executableArgs = getOptionValue(cmd, "args", "");

		envKeys = new java.util.ArrayList<String>();
		envValues = new java.util.ArrayList<String>();
		if (cmd.getOptionValue("envlist") != null) {
			for (String env : cmd.getOptionValue("envlist").split(":")) {
				envKeys.add(env.split("=")[0]);
				envValues.add(env.split("=")[1]);
			}
			//System.out.println("CLASSPATH: " + System.getenv("CLASSPATH"));
		}
		sharedObjects = new HashMap<String, String>();
		if (cmd.getOptionValue("sharedlist") != null) {
			for (String hdfsPathPattern : cmd.getOptionValue("sharedlist").split(":")) {
				final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(hdfsPathPattern));
				for (FileStatus status : fileStatuses) {
					Path path = status.getPath();
					String name = path.getName();
					if (sharedObjects.containsKey(name)) {
						logger.warn("Possible duplicated shared-object entry: " + name);
					}
					sharedObjects.put(name, path.toString());
					logger.debug(String.format("shared object dependency: %s = %s", name, path.toString()));
				}
			}
		}
		additionalResources = new HashMap<String, String>();
		if(cmd.getOptionValue("r") != null) {
			for(String pair : cmd.getOptionValue("r").split(":")) {
				String name = pair.split("=")[0];
				String hdfs_path = pair.split("=")[1];
				additionalResources.put(name, hdfs_path);
				logger.debug(String.format("additional resources: %s = %s", name, hdfs_path));
			}
		}
	}

	private void addOption(Options options, boolean required, String opt, String longopt, boolean hasArg, String description) {
		Option option = new Option(opt, longopt, hasArg, description);
		option.setRequired(required);
		options.addOption(option);
	}

	private String getOptionValue(CommandLine cmd, String key, String defaultValue) {
		if(cmd.getOptionValue(key) != null) {
			return cmd.getOptionValue(key);
		} else {
			return defaultValue;
		}
	}

	// given a path, extract its name
	private String extractNameFromPath(String strpath) {
		Path path = new Path(strpath);
		return path.getName();
	}

	private static void assertPathExist(String path) throws FileNotFoundException {
		File file = new File(path);
		if (!file.exists()) {
			System.out.println("File " + file + " does not exist.");
			throw new FileNotFoundException("File " + file + " does not exist.");
		}
	}

	public static String MY_CONF_SERIALIZED = "MPI_YARN_MY_CONF";

	public static void setupLocalResource(FileSystem dfs, Path path, LocalResource localResource) throws IOException {
		FileStatus fileStat = dfs.getFileStatus(path);
		localResource.setResource(ConverterUtils.getYarnUrlFromPath(path));
		localResource.setSize(fileStat.getLen());
		localResource.setTimestamp(fileStat.getModificationTime());
		localResource.setType(LocalResourceType.FILE);
		localResource.setVisibility(LocalResourceVisibility.PUBLIC);
	}

	public static void main(String[] args) throws Exception {
		String[] a = "AAA BBB CCC".split(" ");
    //System.out.println(String.join(" ", a));
		//System.out.println(StringUtils.join(a, " "));

		System.out.println("Hello");
		// String[] test1 = {"-x", "16"};
		String[] test1 = { "-a", "/bin/ls", "-p", "hdfs://localhost:9000/", "-N", "16", "-jar", "/bin/ls", "-envlist",
				"HOME,USER" };
		MyConf conf = new MyConf(test1);
		conf = MyConf.deserialize(MyConf.serialize(conf));
		System.out.println(conf.getNumNodes());
		System.out.println(conf.getNumProcsPerNode());
		System.out.println(conf.getOutputPath());
		System.out.println(serialize(conf).length());
	}

	public int getLauncherType() {
		return launcherType;
	}

	public YarnConfiguration getYarnConfiguration() {
		return yarnConfiguration;
	}

	public int getVCoresPerNode() {
		return vCoresPerNode;
	}

	public long getMemoryMbPerNode() {
		return memoryMbPerNode;
	}

	public long getNvdimmMbPerNode() {
		return nvdimmMbPerNode;
	}

	public FileSystem getFileSystem() throws IOException {
		return fileSystem;
	}

	// path-related information
	public String getAppName() { return "[MPI]" + getExecutableName(); }
	public String getContainingJarName() { return extractNameFromPath(containingJarClientPath); }
	public String getContainingJarClientPath() { return containingJarClientPath; }
	public String getContainingJarStagingPath() { return fromNameToStagingPath(getContainingJarName()); }
	public String getExecutableName() { return extractNameFromPath(executableClientPath); }
	public String getExecutableClientPath() { return executableClientPath; }
	public String getExecutableStagingPath() { return fromNameToStagingPath(getExecutableName()); }
	public String getProxyName() { return extractNameFromPath(proxyClientPath); }
	public String getProxyClientPath() { return proxyClientPath; }
	public String getProxyStagingPath() { return fromNameToStagingPath(getProxyName()); }
	public String getLauncherName() { return extractNameFromPath(launcherClientPath); }
	public String getLauncherClientPath() { return launcherClientPath; }
	public String getLauncherStagingPath() { return fromNameToStagingPath(getLauncherName()); }

	private String fromNameToStagingPath(String name) {
		 return hdfsPrefix + Path.SEPARATOR + name;
	}

	// add an entry to local resource
	private static void addLocalResource(Map<String, LocalResource> localResources, FileSystem dfs, String localName, String hdfsPath) throws IOException {
		LocalResource resource = Records.newRecord(LocalResource.class);
		setupLocalResource(dfs, new Path(hdfsPath), resource);
		localResources.put(localName, resource);
	}

	// add entries to local resources table
	//   0. jars
	//   1. executable
	//   2. launcher
	//   3. proxy
	void addLocalResources(Map<String, LocalResource> localResources) throws IOException {
		addLocalResource(localResources, fileSystem, getContainingJarName(), getContainingJarStagingPath());
		addLocalResource(localResources, fileSystem, getExecutableName(), getExecutableStagingPath());
		addLocalResource(localResources, fileSystem, getProxyName(), getProxyStagingPath());
		addLocalResource(localResources, fileSystem, getLauncherName(), getLauncherStagingPath());
		for (String name : getAdditionalResource().keySet()) {
			String hdfsPath = getAdditionalResource().get(name);
			addLocalResource(localResources, fileSystem, name, hdfsPath);
		}
		for (String name : getSharedObjects().keySet()) {
			String hdfsPath = getSharedObjects().get(name);
			addLocalResource(localResources, fileSystem, name, hdfsPath);
		}
	}

	// prepare local resources for deamon
	public Map<String,LocalResource> createDeamonLocalResources() throws IOException {
		Map<String, LocalResource> localResources = new HashMap<>();
		addLocalResources(localResources);
		return localResources;
	}

	// prepare local resources for AM
	public Map<String,LocalResource> createAMLocalResources() throws IOException {
		Map<String, LocalResource> localResources = new HashMap<>();
		addLocalResources(localResources);
		return localResources;
	}

	private String[] getYarnClasspathList() {
		return yarnConfiguration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
	}

	private void addClasspathEntry(String classPathEntry, List<String> classPathList) {
		classPathList.add(classPathEntry);
	}

	private void addAMClassPathToEnv(Map<String, String> appMasterEnv) {
		List<String> classPathList = new ArrayList<>();
		addClasspathEntry(Environment.PWD.$$(), classPathList); // PWD
		addClasspathEntry(Environment.PWD.$$() + Path.SEPARATOR + LOCALIZED_CONF_DIR, classPathList); // local config
		addClasspathEntry(Environment.PWD.$$() + Path.SEPARATOR + getContainingJarName(), classPathList); // APP jar
		for (String classPathEntry : getYarnClasspathList()) {
			addClasspathEntry(classPathEntry.trim(), classPathList);
		}
		String classpath = String.join(ApplicationConstants.CLASS_PATH_SEPARATOR, classPathList);
		appMasterEnv.put(Environment.CLASSPATH.name(), classpath);
		logger.debug("CLASSPATH = " + classpath);
	}

	// prepare environment variables for AM
	public Map<String,String> createAMEnvironment() {
		Map<String, String> appMasterEnv = new HashMap<>();
		addAMClassPathToEnv(appMasterEnv);
		appMasterEnv.put(MyConf.MY_CONF_SERIALIZED, MyConf.serialize(this));
		return appMasterEnv;
	}

	/**
	 * Prepare launch command for AM
	 *
	 * 1. Copy from container PWD to a well-known temp dir (due to MPI's requirement)
	 * 2. Launch AM
	 * @return
	 */
	public List<String> createAMCommand() {
		ArrayList<String> javaOpts = new ArrayList<>();
		javaOpts.add("-Xmx" + getAMJavaMemoryMb() + "m");
		Path tmpDir = new Path(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
		javaOpts.add("-Djava.io.tmpdir=" + tmpDir);

		ArrayList<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(String.format("cp %s %s ;", getExecutableName(), getTemporaryPath()).split(MyConf.SPLIT_SPACE)));
		commandList.add(Environment.JAVA_HOME.$$() + "/bin/java");
		commandList.add("-server");
		for (String opt : javaOpts) {
			commandList.add(opt);
		}
		commandList.add("ai.fma.mpi_yarn.ApplicationMaster");
		commandList.add("1>");
		commandList.add(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
		commandList.add("2>");
		commandList.add(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

		return commandList;
	}

	public void setAMResource(Resource capability) {
		if (getLauncherType() == LAUNCHER_TYPE_HYDRA) {
			capability.setMemorySize(amMemoryMb);
			capability.setVirtualCores(1);
		} else if (getLauncherType() == LAUNCHER_TYPE_ORTE) {
			capability.setMemorySize(amMemoryMb + memoryMbPerNode);
			capability.setVirtualCores(vCoresPerNode);
			if (nvdimmMbPerNode > 0) {
				capability.setResourceValue(MyConf.NVDIMM_RESOURCE_NAME, nvdimmMbPerNode);
			}
		} else {
			assert(false);
		}
	}

	/**
	 * Get the number of containers required as deamon
	 */
	public int getNumDeamonContainers() {
		if (getLauncherType() == LAUNCHER_TYPE_HYDRA) {
			return getNumNodes();
		} else if (getLauncherType() == LAUNCHER_TYPE_ORTE) {
			return getNumNodes() - 1;
		} else {
			assert(false);
			return 0;
		}
	}

	public String getHostfileName() {
		return "__mpi_yarn_hostfile";
	}

	public String getTemporaryPath() {
		return "/tmp";
	}
}
