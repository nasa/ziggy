package gov.nasa.ziggy.util.os;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Process-related functions.
 * <p>
 * Uses JNI and kill(2) to send a SIGKILL signal to a process.
 *
 * @author Forrest Girouard
 * @author Sean McCauliff
 */
public class ProcessUtils {
    private static final Logger log = LoggerFactory.getLogger(ProcessUtils.class);

    /**
     * Gets the process id for the current process.
     */
    public static long getPid() {
        long fallback = -1L;
        RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
        String nameStr = rmx.getName();
        final int index = nameStr.indexOf('@');
        if (index < 1) {
            return fallback;
        }
        try {
            return Long.parseLong(nameStr.substring(0, index));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public static synchronized Set<Long> descendantProcessIds() {
        return descendantProcessIds(getPid());
    }

    /**
     * Returns the set of descendant process IDs for a given ancestor ID. Executes recursively to
     * capture children of children and etc.
     */
    public static Set<Long> descendantProcessIds(long ancestorProcessId) {
        Set<Long> processIds = new TreeSet<>();
        ExternalProcess pgrepProcess = ExternalProcess
            .simpleExternalProcess("/usr/bin/pgrep -P " + Long.toString(ancestorProcessId));
        int retCode = pgrepProcess.execute();
        if (retCode == 0) {
            String resultString = pgrepProcess.getStdoutString();
            if (!resultString.isEmpty()) {
                String[] splitProcessIds = resultString.split(System.lineSeparator());
                for (String processIdString : splitProcessIds) {
                    long processId = Long.parseLong(processIdString);
                    processIds.add(processId);
                    Set<Long> childProcessIds = descendantProcessIds(processId);
                    processIds.addAll(childProcessIds);
                }
            }
        }
        return processIds;
    }

    /**
     * Sends SIGTERM to a process specified by its process ID.
     *
     * @return return code of {@link ExternalProcess} instance used to send the message.
     */
    public static int sendSigtermToProcess(long processId) {
        ExternalProcess sigtermProcess = ExternalProcess
            .simpleExternalProcess("/bin/kill -SIGTERM " + Long.toString(processId));
        return sigtermProcess.execute();
    }

    /**
     * Correctly closes all the resources a process uses. Likely you should use this in a finally
     * block
     *
     * @param process This may be null.
     */
    public static void closeProcess(Process process) {
        if (process == null) {
            return;
        }

        FileUtil.close(process.getOutputStream());
        FileUtil.close(process.getInputStream());
        FileUtil.close(process.getErrorStream());
    }

    /**
     * Runs a Java process using the same environment as the calling Java process. This assumes the
     * command to run the virtual machine is just "java".
     *
     * @param mainClass The class containing the main() method.
     * @param mainArgs The parameters to pass to the class's main method.
     * @return a {@link Process} object.
     * @throws IOException if the process could not be started.
     */
    public static Process runJava(Class<?> mainClass, List<String> mainArgs) throws IOException {

        String className = mainClass.getCanonicalName();
        RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
        String classPath = rmx.getClassPath();
        List<String> javaCommandLineParameters = rmx.getInputArguments();
        String javaExe = SystemUtils.JAVA_HOME + File.separator + "bin" + File.separator + "java";

        List<String> commandList = new ArrayList<>();
        StringBuilder cmd = new StringBuilder();
        cmd.append(javaExe).append(' ');
        commandList.add(javaExe);
        cmd.append("-cp ").append(classPath).append(' ');
        commandList.add("-cp");
        commandList.add(classPath);

        for (String javaArg : javaCommandLineParameters) {
            // don't run the java child with debugging parameters.
            if (javaArg.equals("-Xdebug") || javaArg.startsWith("-Xrunjdwp")
                || javaArg.startsWith("-agentlib:jdwp")
                || javaArg.contains("BootstrapSecurityManager")) {
                continue;
            }
            cmd.append(javaArg).append(' ');
            commandList.add(javaArg);
        }

        cmd.append(className).append(' ');
        commandList.add(className);

        for (String arg : mainArgs) {
            cmd.append(arg).append(' ');
            commandList.add(arg);
        }

        String[] commandArray = new String[commandList.size()];
        commandList.toArray(commandArray);

        log.info("Executing java process with command line \"" + cmd + "\".");
        Process process = Runtime.getRuntime().exec(commandArray);
        BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        // Report stderr if the process fails to launch. Unfortunately, if there
        // is a problem here, it seems you have to be stepping in the debugger
        // in order for errors.ready() to return true so that output can be
        // seen.
        while (errors.ready()) {
            String errString = errors.readLine();
            log.error(errString);
        }

        return process;
    }

}
