package gov.nasa.ziggy.services.process;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

/**
 * Subclass of the Apache {@link DefaultExecutor} that captures the PID of the process run by the
 * executor, and provides the {@link #getProcessId()} method to return same.
 * <p>
 * The only public constructor for DefaultExecutor is its no-arg constructor, which is deprecated.
 * Until such time as DefaultExecutor provides another public constructor or access to the
 * underlying Java Process, we will have to live with this.
 */
@SuppressWarnings("deprecation")
public class ZiggyProcessExecutor extends DefaultExecutor {

    private long processId;
    private CountDownLatch launchLatch = new CountDownLatch(1);

    @Override
    public Process launch(final CommandLine command, final Map<String, String> env, final File dir)
        throws IOException {
        Process process = super.launch(command, env, dir);
        processId = process.pid();
        launchLatch.countDown();
        return process;
    }

    public long getProcessId() {
        try {
            launchLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return processId;
    }
}
