package ai.fma.mpi_yarn;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {

	Configuration conf = new YarnConfiguration();
	FileSystem dfs;
	MyConf myConf;

	private void log(String str) {
		System.out.println("[CLIENT] " + str + "\n");
	}

	public void run(String[] args) throws Exception {
		dfs = FileSystem.get(conf);
		myConf = new MyConf(args);
		Path hdfsPrefix = new Path(myConf.getHdfsPrefix());
		if (!dfs.exists(hdfsPrefix)) {
			throw new RuntimeException("hdfsPrefix " + myConf.getHdfsPrefix() + " does not exist.");
		}

		// Create yarnClient
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		// Create application via yarnClient
		YarnClientApplication app = yarnClient.createApplication();

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		amContainer.setCommands(Collections
				.singletonList("$JAVA_HOME/bin/java" + " -Xmx" + String.valueOf(myConf.getContainerMemoryMb()) + "M"
						+ " ai.fma.mpi_yarn.ApplicationMaster" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
						+ "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

		// Copy required file

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		{
			Path localJarPath = new Path(myConf.getContainingJar());
			Path hdfsJarPath = new Path(hdfsPrefix + "/mpi_yarn_am.jar");
			log("copy local jar file " + myConf.getContainingJar() + " into " + hdfsJarPath.toUri().toString());
			dfs.copyFromLocalFile(false, true, localJarPath, hdfsJarPath);
			LocalResource appMasterJar = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, hdfsJarPath, appMasterJar);
			localResources.put(hdfsJarPath.getName(), appMasterJar);
		}
		{
			Path executablePath = new Path(hdfsPrefix + "/" + myConf.getExecutableName());
			log("copy executable file " + myConf.getExecutablePath() + " into " + executablePath.toUri().toString());
			dfs.copyFromLocalFile(false, true, new Path(myConf.getExecutablePath()), executablePath);
		}
		{
			Path proxyPath = new Path(hdfsPrefix + "/" + MyConf.PMI_PROXY);
			log("copy hydra proxy " + myConf.getHydraProxy() + " into " + proxyPath.toUri().toString());
			dfs.copyFromLocalFile(false, true, new Path(myConf.getHydraProxy()), proxyPath);
			// mpiexec should be a resource for AM
			Path mpiexecPath = new Path(hdfsPrefix + "/" + MyConf.MPIEXEC);
			log("copy mpiexec " + myConf.getHydraMpiexec() + " into " + mpiexecPath.toUri().toString());
			dfs.copyFromLocalFile(false, true, new Path(myConf.getHydraMpiexec()), mpiexecPath);
			LocalResource mpiexecResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, mpiexecPath, mpiexecResource);
			localResources.put(MyConf.MPIEXEC, mpiexecResource);
		}

		{
			String testSS = MyConf.testSS;
			Path testPath = new Path(testSS);
			//log("copy testing shell script" + testSS " into " + testPath.toUri().toString());
			dfs.copyFromLocalFile(false, true, new Path(testSS), testPath);
			LocalResource testResource = Records.newRecord(LocalResource.class);
			MyConf.setupLocalResource(dfs, testPath, testResource);
			localResources.put(testSS, testResource);
		}

		Path soPrefix = new Path(hdfsPrefix + "/sofiles/");
		dfs.mkdirs(soPrefix);
		for (String sofile : myConf.getSharedObjectPathList()) {
			Path src = new Path(sofile);
			Path target = new Path(hdfsPrefix + "/sofiles/" + src.getName());
			log("copy shared object file " + src.toUri().toString() + " into " + target.toUri().toString());
			dfs.copyFromLocalFile(false, true, src, target);
		}
		Path sfPrefix = new Path(hdfsPrefix + "/sf/");
		dfs.mkdirs(sfPrefix);
		for (int k = 0; k < myConf.getLocalPathSF().size();k++) {
			Path src = new Path(myConf.getLocalPathSF().get(k));
			Path target = new Path(hdfsPrefix + "/sf/" + myConf.getRemotePathSF().get(k));
			log("copy supplementary file" + src.toUri().toString() + " into " + target.toUri().toString());
			dfs.copyFromLocalFile(false, true, src, target);
		}
		
		// Setup jar for ApplicationMaster

		amContainer.setLocalResources(localResources);

		// Setup CLASSPATH for ApplicationMaster
		Map<String, String> appMasterEnv = new HashMap<String, String>();
		StringBuilder classPathEnv = new StringBuilder("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(':');
			classPathEnv.append(c.trim());
		}
		HashSet<String> envList = myConf.getEnvList();
		appMasterEnv.put("JAVA_HOME", System.getenv("JAVA_HOME"));		
		appMasterEnv.put("PATH", System.getenv("PATH"));
		//appMasterEnv.put("CLASSPATH", System.getenv("CLASSPATH") + ":" + classPathEnv.toString());
		for (int k = 0;k < myConf.getEnvHomeList().size();k++) {
				String tempEnv = System.getenv(myConf.getEnvHomeList().get(k)) + ":" + myConf.getEnvHomeEnv().get(k);
				appMasterEnv.put(myConf.getEnvHomeList().get(k), tempEnv);
		}
		if(appMasterEnv.get("CLASSPATH") == null) {
			appMasterEnv.put("CLASSPATH", System.getenv("CLASSPATH") + ":" + classPathEnv.toString());
		}
		else {
			appMasterEnv.put("CLASSPATH", appMasterEnv.get("CLASSPATH") + ":" + classPathEnv.toString());
		}
		//System.out.println("\nCLIENT CLASSPATH: " + System.getenv("CLASSPATH"));
		//appMasterEnv.put("CLASSPATH", System.getenv("CLASSPATH") + ":" + classPathEnv.toString());
		//System.out.println("CLIENT CLASSPATH: " + System.getenv("CLASSPATH"));
		// convey MyConf to AM via environment variable
		
		appMasterEnv.put(MyConf.EnvName, MyConf.serialize(myConf));
		log("Environment: " + appMasterEnv.toString());
		amContainer.setEnvironment(appMasterEnv);
		
		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(myConf.getContainerMemoryMb());
		capability.setVirtualCores(1);

		// Finally, set-up ApplicationSubmissionContext for the application
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName("mpi_yarn"); // application name
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		appContext.setQueue(myConf.getQueueName()); // queue

		// Submit application
		ApplicationId appId = appContext.getApplicationId();
		System.out.println("submitting application " + appId + " to queue " + myConf.getQueueName());
		System.out.println("output location = " + myConf.getOutputPath());
		yarnClient.submitApplication(appContext);

		ApplicationReport appReport = yarnClient.getApplicationReport(appId);
		YarnApplicationState appState = appReport.getYarnApplicationState();
		long outputOffset = 0;
		int bufferSize = 1024 * 1024;
		byte[] buffer = new byte[bufferSize];
		while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED
				&& appState != YarnApplicationState.FAILED) {
			Thread.sleep(100);
			appReport = yarnClient.getApplicationReport(appId);
			appState = appReport.getYarnApplicationState();
			Path outputPath = new Path(myConf.getOutputPath());
			if (dfs.exists(outputPath)) {
				FSDataInputStream inputStream = dfs.open(outputPath);
				int readBytes = inputStream.read(outputOffset, buffer, 0, bufferSize);
				if (readBytes != -1) {
					String nextChunk = new String(buffer, 0, readBytes, StandardCharsets.UTF_8);
					System.out.print(nextChunk);
					outputOffset += readBytes;
				}
				inputStream.close();
			}
		}

		// use sleep because we have to wait for AM's write to HDFS visible
		Thread.sleep(3000);
		Path outputPath = new Path(myConf.getOutputPath());
		if (dfs.exists(outputPath)) {
			while (true) {
				FSDataInputStream inputStream = dfs.open(outputPath);
				int readBytes = inputStream.read(outputOffset, buffer, 0, bufferSize);
				if (readBytes != -1) {
					String nextChunk = new String(buffer, 0, readBytes, StandardCharsets.UTF_8);
					System.out.print(nextChunk);
					outputOffset += readBytes;
				} else {
					inputStream.close();
					break;
				}
			}
		}

		System.out.println(
				"Application " + appId + " finished with" + " state " + appState + " at " + appReport.getFinishTime());

	}

	public static void main(String[] args) throws Exception {
		Client c = new Client();
		c.run(args);
	}
}
