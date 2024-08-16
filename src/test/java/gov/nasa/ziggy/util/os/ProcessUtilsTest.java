package gov.nasa.ziggy.util.os;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * @author Sean McCauliff
 */
public class ProcessUtilsTest {
    private Process subJavaProcess;

    @After
    public void cleanUpProcesses() {
        if (subJavaProcess != null) {
            ProcessUtils.closeProcess(subJavaProcess);
            subJavaProcess = null;
        }
    }

    /**
     * This is kind of a weak test since we can't really know the correct value.
     *
     * @throws Exception
     */
    @Test
    public void testPid() throws Exception {
        long pid = ProcessUtils.getPid();
        assertTrue(pid > 0); // 0 is init on UNIX
        assertTrue(pid <= OperatingSystemType.getInstance().getProcInfo().getMaximumPid());
        ExternalProcess psProcess = ExternalProcess
            .simpleExternalProcess("/bin/ps -o pid,comm " + Long.toString(pid));
        psProcess.execute();
        String psString = psProcess.getStdoutString();
        assertTrue(psString.endsWith("java\n"));
    }

    @Test
    public void testRunJava() throws Exception {
        subJavaProcess = ProcessUtils.runJava(ProcessUtilsTestProgram.class,
            Arrays.asList("44", "Hello world."));
        subJavaProcess.waitFor();

        InputStream inputStream = subJavaProcess.getInputStream();
        BufferedReader bufferedReader = new BufferedReader(
            new InputStreamReader(inputStream, ZiggyFileUtils.ZIGGY_CHARSET));
        String helloWorld = bufferedReader.readLine();
        inputStream.close();
        InputStream errorStream = subJavaProcess.getErrorStream();
        if (errorStream.available() > 0) {
            StringBuilder builder = new StringBuilder();
            ZiggyFileUtils.readAll(builder, errorStream);
            System.out.println(builder.toString());
        }
        subJavaProcess.getErrorStream().close();
        subJavaProcess.getOutputStream().close();
        int exitValue = subJavaProcess.waitFor();

        assertEquals("Hello world.", helloWorld);
        assertEquals(44, exitValue);
    }

    @Test
    public void testChildProcesses() throws InterruptedException {

        Set<Long> childProcessIds = ProcessUtils.descendantProcessIds();
        assertEquals(0, childProcessIds.size());
        int nProcesses = 2;
        for (int i = 0; i < nProcesses; i++) {
            ExternalProcess psProcess = ExternalProcess.simpleExternalProcess("/bin/sleep 1");
            psProcess.timeout(1000);
            psProcess.execute(false);
        }
        TestEventDetector.detectTestEvent(500L,
            () -> ProcessUtils.descendantProcessIds().size() >= nProcesses);
        childProcessIds = ProcessUtils.descendantProcessIds();
        assertEquals(nProcesses, childProcessIds.size());
        for (long processId : childProcessIds) {
            ProcessUtils.sendSigtermToProcess(processId);
        }
        TestEventDetector.detectTestEvent(500L,
            () -> ProcessUtils.descendantProcessIds().size() == 0);
        assertEquals(0, ProcessUtils.descendantProcessIds().size());
    }
}
