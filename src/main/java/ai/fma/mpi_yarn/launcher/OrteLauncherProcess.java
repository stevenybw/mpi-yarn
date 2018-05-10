package ai.fma.mpi_yarn.launcher;

import ai.fma.mpi_yarn.MyConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class OrteLauncherProcess extends LauncherProcess {
    static Logger logger = LoggerFactory.getLogger(OrteLauncherProcess.class);

    private Process process;
    private InputStream stdout;
    private InputStream stderr;
    private Scanner stdout_scanner;
    private Scanner stderr_scanner;
    private Map<String, String[]> deamonHostCommand;

    public OrteLauncherProcess(MyConf myConf, Set<String> hosts, Set<String> deamons) throws IOException {
        super();
        String hostfile = myConf.getHostfileName();
        int npernode = myConf.getNumProcsPerNode();
        dumpHostfile(hostfile, hosts, npernode);
        String command = String.format("./%s -mca plm_rsh_no_tree_spawn true -mca plm_rsh_agent echo -npernode %d -hostfile %s -- /tmp/%s %s", myConf.getLauncherName(), npernode, hostfile, myConf.getExecutableName(), myConf.getExecutableArgs());
        logger.info("Start launcher via command: " + command);
        ProcessBuilder pb = new ProcessBuilder(command.split("\\s"));
        process = pb.start();
        stdout = process.getInputStream();
        stderr = process.getErrorStream();
        stdout_scanner = new Scanner(stdout);
        stderr_scanner = new Scanner(stderr);
        deamonHostCommand = new HashMap<>();
        Set<String> processedHosts = new HashSet<>();
        for(int i=0; i<deamons.size(); i++) {
            // replace all " to \"
            String line = stdout_scanner.nextLine().replaceAll("--daemonize","").replace("\"", "\\\"");
            String[] line_sp = line.split("\\s+");
            String host = line_sp[0];
            String[] deamonCommand = Arrays.copyOfRange(line_sp, 1, line_sp.length);
            deamonCommand[0] = "./" + deamonCommand[0];
            deamonHostCommand.put(host, deamonCommand);
            logger.info("  Expect Args from stdout: " + line);
            logger.info("    host = " + host);
            logger.info("    command = " + String.join(" ", deamonCommand));
        }
        logger.info("Necessary information to launch container has been gathered");
    }

    private void dumpHostfile(String hostfile, Set<String> hosts, int npernode) {
        logger.info(String.format("hosts = [%s]   npernode = %d", String.join(", ", hosts), npernode));
        List<String> lines = new ArrayList<>();
        for (String host : hosts) {
            lines.add(String.format("%s max_slots=%d", host, npernode));
        }
        Path file = Paths.get(hostfile);
        try {
            Files.write(file, lines, Charset.forName("UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
            assert(false);
        }
        for (String line : lines) {
            logger.info(line);
        }
        logger.info("Hostfile dumped to " + hostfile);
    }

    @Override
    public InputStream getInputStream() {
        return stdout;
    }

    @Override
    public InputStream getErrorStream() {
        return stderr;
    }

    @Override
    public Map<String, String[]> getDeamonHostCommand(MyConf myConf) {
        return deamonHostCommand;
    }
}
