package gov.nasa.ziggy.buildutil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;

/***
 * Gradle has an ExecTask, but you can't subclass it to make your own tasks.
 *
 * @author Sean McCauliff
 * @author Bill Wohler
 */
public abstract class ZiggyDefaultTask extends DefaultTask {

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
