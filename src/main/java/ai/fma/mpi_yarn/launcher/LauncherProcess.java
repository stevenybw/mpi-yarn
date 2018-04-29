package ai.fma.mpi_yarn.launcher;

import ai.fma.mpi_yarn.MyConf;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.InputStream;
import java.util.Map;

public abstract class LauncherProcess {
    // Get the InputStream from stdout
    public abstract InputStream getInputStream();

    // Get the InputStream from stderr
    public abstract InputStream getErrorStream();

    // Get the mapping from hostname to deamon command
    public abstract Map<String,String> getDeamonHostCommand(MyConf myConf);
}
