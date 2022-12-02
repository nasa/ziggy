package gov.nasa.ziggy.buildutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;

/***
 * Gradle has an ExecTask, but you can't subclass it to make your own tasks.
 *
 * @author Sean McCauliff
 */
abstract class TessExecTask extends DefaultTask {

    protected static boolean isMccEnabled() {
        String mccEnabled = System.getenv("MCC_ENABLED");
        if (mccEnabled == null || !Boolean.valueOf(mccEnabled)) {
            return false;
        }
        return true;
    }

    protected static File matlabHome() {
        String matlabHome = System.getenv("MATLAB_HOME");
        if (matlabHome == null) {
            throw new GradleException("MATLAB_HOME is not set");
        }
        if (matlabHome.contains("2010")) {
            throw new GradleException(
                "MATLAB_HOME=" + matlabHome + ".  This is not the MATLAB I'm looking for.");
        }
        File home = new File(matlabHome);
        if (!home.exists()) {
            throw new GradleException("MATLAB_HOME=\"" + home + "\" does not exist.");
        }
        return home;
    }

    /**
     * @param command The first element in this list is the name of the executable. the others are
     * arguments. No escaping is required.
     */
    protected static void execProcess(ProcessBuilder processBuilder) {

        processBuilder.redirectErrorStream(true);
        String commandString = processBuilder.command().stream().collect(Collectors.joining(" "));
        try {
            Process process = processBuilder.start();
            process.waitFor();
            if (process.exitValue() != 0) {
                try (InputStreamReader inRead = new InputStreamReader(process.getInputStream());
                    BufferedReader breader = new BufferedReader(inRead)) {
                    StringBuilder bldr = new StringBuilder();
                    bldr.append(commandString).append('\n');
                    for (String line = breader.readLine(); line != null; line = breader
                        .readLine()) {
                        bldr.append(line).append("\n");
                    }
                    throw new GradleException(bldr.toString());
                } catch (IOException ioe) {
                    throw new GradleException("Command \"" + commandString
                        + "\" Failed, no other information avaialble.");
                }
            }
        } catch (IOException e) {
            throw new GradleException("While trying to exec \"" + commandString + "\".", e);
        } catch (InterruptedException e) {
            throw new GradleException(commandString, e);
        }
    }
}
