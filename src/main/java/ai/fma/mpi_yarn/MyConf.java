
package ai.fma.mpi_yarn;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class MyConf implements Serializable {
	public static String MPIEXEC = "mpiexec.hydra";
	public static String PMI_PROXY = "hydra_pmi_proxy";
	public static String testSS = "mytest.sh";

	public String getHydraPrefix() {
		return hydraPrefix;
	}

	public String getHydraMpiexec() {
		return hydraPrefix + "/" + MPIEXEC;
	}

	public String getHydraProxy() {
		return hydraPrefix + "/" + PMI_PROXY;
	}

	public static String serialize(MyConf conf) {
		byte[] data = SerializationUtils.serialize(conf);
		return Base64.encodeBase64String(data);
	}

	public static MyConf deserialize(String base64String) {
		byte[] data = Base64.decodeBase64(base64String);
		return (MyConf) SerializationUtils.deserialize(data);
	}

	public String getQueueName() {
		return queueName;
	}

	public int getContainerMemoryMb() {
		return containerMemoryMb;
	}

	public Date getNow() {
		return now;
	}

	public String getHdfsPrefix() {
		return hdfsPrefix;
	}

	public HashSet<String> getEnvList() {
		return envList;
	}

	public String getExecutablePath() {
		return executablePath;
	}

	public String getExecutableArgs() {
		return executableArgs;
	}

	public File getExecutableFile() {
		return executableFile;
	}

	public String getExecutableName() {
		return executableName;
	}

	public List<String> getSharedObjectPathList() {
		return sharedObjectPathList;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public LocalityType getLocalityType() {
		return localityType;
	}

	public int getNumProcs() {
		return numProcs;
	}

	public int getNumNodes() {
		return numNodes;
	}

	public int getNumProcsPerNode() {
		return numProcsPerNode;
	}

	public String getContainingJar() {
		return containingJar;
	}
	public List<String> getLocalPathSF() {
		return localPathSF;
	}
	public List<String> getRemotePathSF() {
		return remotePathSF;
	}
	public List<String> getEnvHomeList() {
		return envHomeList;
	}
	public List<String> getEnvHomeEnv() {
		return envHomeEnv;
	}
	public MyConf(String[] args) {
		now = new Date();

		Options options = new Options();

		Option optionSupplementPath = new Option("sp", "", true, "path of the directory that contains supplmentary files");
		optionSupplementPath.setRequired(false);
		options.addOption(optionSupplementPath);

		Option optionExecutablePath = new Option("a", "", true, "path to the MPI executable");
		optionExecutablePath.setRequired(true);
		options.addOption(optionExecutablePath);

		Option optionExecutableArgs = new Option("args", "", true, "argument to the MPI executable");
		optionExecutableArgs.setRequired(false);
		options.addOption(optionExecutableArgs);

		Option supplementPathArgs = new Option("sf", "", true, "path of additional/supplementary files.");
		supplementPathArgs.setRequired(false);
		options.addOption(supplementPathArgs);

		Option optionHdfsPrefix = new Option("p", "", true, "HDFS prefix");
		optionHdfsPrefix.setRequired(true);
		options.addOption(optionHdfsPrefix);

		Option optionHydraHome = new Option("hydra", "", true, "the prefix of hydra process manager");
		optionHydraHome.setRequired(true);
		options.addOption(optionHydraHome);

		Option optionNumProcs = new Option("n", "", true, "num processes");
		optionNumProcs.setRequired(false);
		options.addOption(optionNumProcs);

		Option optionNumNodes = new Option("N", "", true, "num nodes");
		optionNumNodes.setRequired(false);
		options.addOption(optionNumNodes);

		Option optionOutputPath = new Option("o", "", true, "output file name");
		optionOutputPath.setRequired(false);
		options.addOption(optionOutputPath);

		Option optionNumProcsPerNode = new Option("ppn", true, "num processes per node");
		optionNumProcsPerNode.setRequired(false);
		options.addOption(optionNumProcsPerNode);

		Option optionEnvList = new Option("envlist", true, "comma(,)-seperated list of environment variable to pass");
		optionEnvList.setRequired(false);
		options.addOption(optionEnvList);

		Option optionSharedList = new Option("sharedlist", true,
				"comma(,)-seperated list of the path of shared objects");
		optionSharedList.setRequired(false);
		options.addOption(optionSharedList);

		Option optionJarFile = new Option("jar", true, "the jar file");
		optionJarFile.setRequired(true);
		options.addOption(optionJarFile);

		Option optionMemoryMb = new Option("m", true, "container memory in MB");
		optionMemoryMb.setRequired(false);
		options.addOption(optionMemoryMb);

		Option optionQueueName = new Option("q", true, "queue name");
		optionQueueName.setRequired(false);
		options.addOption(optionQueueName);

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
		executablePath = cmd.getOptionValue("a");
		executableFile = new File(executablePath);
		if (!executableFile.exists()) {
			System.out.println("Executable " + executablePath + " does not exist.");
			formatter.printHelp("mpi-run", options);
			System.exit(1);
		}
		executableName = executableFile.getName();
		if (cmd.getOptionValue("args") != null) {
			executableArgs = cmd.getOptionValue("args");
		} else {
			executableArgs = "";
		}
		hdfsPrefix = cmd.getOptionValue("p");
		hydraPrefix = cmd.getOptionValue("hydra");

		{
			File file = new File(getHydraMpiexec());
			if (!file.exists()) {
				System.out.println("Hydra mpiexec " + getHydraMpiexec() + " does not exist.");
				formatter.printHelp("mpi-run", options);
				System.exit(1);
			}
		}

		{
			File file = new File(getHydraProxy());
			if (!file.exists()) {
				System.out.println("Hydra pmi_proxy " + getHydraMpiexec() + " does not exist.");
				formatter.printHelp("mpi-run", options);
				System.exit(1);
			}
		}

		if (cmd.getOptionValue("n") != null) {
			numProcs = Integer.valueOf(cmd.getOptionValue("n"));
		} else {
			numProcs = -1;
		}
		if (cmd.getOptionValue("N") != null) {
			numNodes = Integer.valueOf(cmd.getOptionValue("N"));
		} else {
			numNodes = -1;
		}
		if (cmd.getOptionValue("o") != null) {
			outputPath = cmd.getOptionValue("o");
		} else {
			outputPath = hdfsPrefix + "/output." + executableName + "." + String.valueOf(1900 + now.getYear()) + "_"
					+ String.valueOf(now.getMonth()) + "_" + String.valueOf(now.getDay()) + "_"
					+ String.valueOf(now.getHours()) + "_" + String.valueOf(now.getMinutes()) + "_"
					+ String.valueOf(now.getSeconds()) + ".txt";
		}
		if (cmd.getOptionValue("ppn") != null) {
			numProcsPerNode = Integer.valueOf(cmd.getOptionValue("ppn"));
		} else { 
			numProcsPerNode = 1;
		}

		envHomeList = new java.util.ArrayList<String>();
		envHomeEnv = new java.util.ArrayList<String>();
		envList = new HashSet<String>();
		if (cmd.getOptionValue("envlist") != null) {
			for (String env : cmd.getOptionValue("envlist").split(",")) {
				envHomeList.add(env.split("=")[0]);
				envHomeEnv.add(env.split("=")[1]);
			}
			//System.out.println("CLASSPATH: " + System.getenv("CLASSPATH"));
		}
		sharedObjectPathList = new java.util.ArrayList<String>();
		if (cmd.getOptionValue("sharedlist") != null) {
			for (String path : cmd.getOptionValue("sharedlist").split(",")) {
				sharedObjectPathList.add(path);
			}
		}

		localPathSF = new java.util.ArrayList<String>();
		remotePathSF = new java.util.ArrayList<String>();
		if(cmd.getOptionValue("sf") != null) {
			for(String pair : cmd.getOptionValue("sf").split(",")) {
				localPathSF.add(pair.split(":")[0]);
				//System.out.println(pair.split(":")[0]);
				remotePathSF.add(pair.split(":")[1]);
				//System.out.println(pair.split(":")[1]);
			}
		}

		containingJar = cmd.getOptionValue("jar");
		File containingJarFile = new File(containingJar);
		if (!containingJarFile.exists()) {
			System.out.println("Jar " + containingJar + " does not exist.");
			formatter.printHelp("mpi-run", options);
			System.exit(1);
		}

		if (cmd.getOptionValue("m") != null) {
			containerMemoryMb = Integer.valueOf(cmd.getOptionValue("m"));
		} else {
			containerMemoryMb = 256;
		}

		if (cmd.getOptionValue("q") != null) {
			queueName = cmd.getOptionValue("q");
		} else {
			queueName = "default";
		}

		// assert
		if (numProcs == -1 && numNodes == -1 || numProcs != -1 && numNodes != -1) {
			System.out.println("Exactly one of -n, -N must be set.");
			formatter.printHelp("mpi-run", options);
			System.exit(1);
			return;
		} else if (numProcs != -1) {
			localityType = LocalityType.NONE;
		} else {
			localityType = LocalityType.GROUP;
		}
	}

	private Date now;
	private String hdfsPrefix;
	private String hydraPrefix;
	private HashSet<String> envList;
	private List<String> envHomeList;
	private List<String> envHomeEnv;
	private String executablePath;
	private List<String> localPathSF;
	private List<String> remotePathSF;
	private String executableArgs;
	private File executableFile;
	private String executableName;
	private List<String> sharedObjectPathList;
	private String outputPath;
	private LocalityType localityType;
	private int numProcs;
	private int numNodes;
	private int numProcsPerNode;
	private String containingJar;
	private int containerMemoryMb;
	private String queueName;

	public static String EnvName = "MPI_YARN_MY_CONF";

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
		System.out.println(conf.getNumProcs());
		System.out.println(conf.getNumProcsPerNode());
		System.out.println(conf.getEnvList());
		System.out.println(conf.getLocalityType());
		System.out.println(conf.getOutputPath());
		System.out.println(serialize(conf).length());
	}
}
