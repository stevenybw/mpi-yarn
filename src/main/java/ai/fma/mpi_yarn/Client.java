package ai.fma.mpi_yarn;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    Logger logger = LoggerFactory.getLogger(Client.class);

    MyConf myConf;
	YarnConfiguration conf;
	FileSystem dfs;

	public void run(String[] args) throws Exception {
	    // Create yarnClient
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		// Create application via yarnClient
		YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse getNewApplicationResponse = app.getNewApplicationResponse();
        myConf = new MyConf(args, getNewApplicationResponse);
        conf = myConf.getYarnConfiguration();
        dfs = myConf.getFileSystem();

        uploadFilesToStagingDirectory();
        Map<String, LocalResource> localResources = myConf.createAMLocalResources();
        Map<String, String> appMasterEnv = myConf.createAMEnvironment();
        String amCommand = myConf.createAMCommand();
        ContainerLaunchContext amContext = createAMContainerLaunchContext(getNewApplicationResponse);

        Map<String, LocalResource> localResources = createAMLocalResources();

		// Setup jar for ApplicationMaster

		amContainer.setLocalResources(localResources);
		
		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(myConf.getAMResourceMemoryMb());
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

    // upload required files into staging directory
    //   1. AM Jar
    //   2. Executable
    //   3. Proxy
    //   4. Launcher
    // TODO Make them all optional
    private void uploadFilesToStagingDirectory() throws IOException {
	    uploadToHDFS(myConf.getContainingJarClientPath(), myConf.getContainingJarStagingPath());
	    uploadToHDFS(myConf.getExecutableClientPath(), myConf.getExecutableStagingPath());
	    uploadToHDFS(myConf.getProxyClientPath(), myConf.getProxyStagingPath());
	    uploadToHDFS(myConf.getLauncherClientPath(), myConf.getLauncherStagingPath());
    }

    private void uploadToHDFS(String clientPath, String hdfsPath) throws IOException {
	    logger.debug(String.format("Upload client file (%s) to HDFS (%s)", clientPath, hdfsPath));
	    dfs.copyFromLocalFile(false, true, new Path(clientPath), new Path(hdfsPath));
    }

    public static void main(String[] args) throws Exception {
		Client c = new Client();
		c.run(args);
	}
}
