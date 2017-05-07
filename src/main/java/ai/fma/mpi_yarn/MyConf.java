
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class MyConf implements Serializable {
	
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

	public MyConf(String[] args) {
		now = new Date();
		
		Options options = new Options();

		Option optionExecutablePath = new Option("a", "", true, "path to the MPI executable");
		optionExecutablePath.setRequired(true);
		options.addOption(optionExecutablePath);
		
		Option optionExecutableArgs = new Option("args", "", true, "argument to the MPI executable");
		optionExecutableArgs.setRequired(false);
		options.addOption(optionExecutableArgs);
		
		Option optionHdfsPrefix = new Option("p", "", true, "HDFS prefix");
		optionHdfsPrefix.setRequired(true);
		options.addOption(optionHdfsPrefix);
		
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
		
		Option optionSharedList = new Option("sharedlist", true, "comma(,)-seperated list of the path of shared objects");
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
			cmd = parser.parse(options,  args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("mpi-run", options);
			System.exit(1);
			return;
		}
		
		// process
		executablePath = cmd.getOptionValue("a");
		executableArgs = cmd.getOptionValue("args");
		hdfsPrefix = cmd.getOptionValue("p");
		if(cmd.getOptionValue("n") != null) {
			numProcs = Integer.valueOf(cmd.getOptionValue("n"));
		} else {
			numProcs = -1;
		}
		if(cmd.getOptionValue("N") != null) {
			numNodes = Integer.valueOf(cmd.getOptionValue("N"));
		} else {
			numNodes = -1;
		}
		if(cmd.getOptionValue("o") != null) {
			outputPath = cmd.getOptionValue("o");
		} else {
			outputPath = "output." + executableName + "." + String.valueOf(1900 + now.getYear())  + "_" + String.valueOf(now.getMonth())
			+ "_" + String.valueOf(now.getDay()) + "_" + String.valueOf(now.getHours()) + "_" + String.valueOf(now.getMinutes())
			+ "_" + String.valueOf(now.getSeconds()) + ".txt";
		}
		if(cmd.getOptionValue("ppn") != null) {
			numProcsPerNode = Integer.valueOf(cmd.getOptionValue("ppn")); 
		} else {
			numProcsPerNode = 1;
		}
		envList = new HashSet<String>();
		if(cmd.getOptionValue("envlist") != null) {
			for(String env : cmd.getOptionValue("envlist").split(",")) {
				envList.add(env);
			}
		}
		sharedObjectPathList = new java.util.ArrayList<String>();
		if(cmd.getOptionValue("sharedlist") != null) {
			for(String path : cmd.getOptionValue("sharedlist").split(",")) {
				sharedObjectPathList.add(path);
			}
		}
		
		executableFile = new File(executablePath);
		executableName = executableFile.getName();
		if(! executableFile.exists()) {
			System.out.println("Executable " + executablePath + " does not exist.");
			formatter.printHelp("mpi-run", options);
			System.exit(1);
		}
		
		containingJar = cmd.getOptionValue("jar");
		File containingJarFile = new File(containingJar);
		if(! containingJarFile.exists()) {
			System.out.println("Jar " + containingJar + " does not exist.");
			formatter.printHelp("mpi-run", options);
			System.exit(1);
		}
		
		if(cmd.getOptionValue("m") != null) {
			containerMemoryMb = Integer.valueOf(cmd.getOptionValue("m"));
		} else {
			containerMemoryMb = 256;
		}
		
		if(cmd.getOptionValue("q") != null) {
			queueName = cmd.getOptionValue("q");
		} else {
			queueName = "default";
		}
		
		// assert
		if(numProcs == -1 && numNodes == -1 || numProcs != -1 && numNodes != -1) {
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
	private HashSet<String> envList;
	private String executablePath;
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
		System.out.println(MessageFormat.format("String is {0}, Int is {1}", "AAA", 123));
		
		System.out.println("Hello");
		//String[] test1 = {"-x", "16"};
		String[] test1 = {"-a", "/bin/ls", "-p", "hdfs://localhost:9000/", "-N", "16", "-jar", "/bin/ls", "-envlist", "HOME,USER"};
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
