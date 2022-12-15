package gov.nasa.ziggy.services.process;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.exec.CommandLine;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.logging.PlainTextLogOutputStream;
import gov.nasa.ziggy.services.logging.WriterLogOutputStream;

public class ExternalProcessTest {

    private File exeDir;
    private File exe;
    private File workingDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void before() throws Exception {
        workingDir = directoryRule.directory().toFile();
        exeDir = Paths.get("build", "bin").toFile().getCanonicalFile();
        exe = new File(exeDir, "testprog");
        if (!exe.exists()) {
            throw new IllegalStateException("Can't find test program \"" + exe + "\".");
        }
    }

    private CommandLine command(int retcode, int sleeptime, boolean crashFlag, boolean touchFile)
        throws IOException {
        CommandLine command = new CommandLine(exe.getCanonicalPath());
        command.addArgument(Integer.toString(retcode));
        command.addArgument(Integer.toString(sleeptime));
        command.addArgument(crashFlag ? "1" : "0");
        command.addArgument(touchFile ? "1" : "0");
        return command;
    }

    @Test
    public void testNormalSyncRun() throws Exception {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(command(0, 0, false, false));
        p.setWorkingDirectory(exeDir);
        p.logStdOut(true);
        p.logStdErr(true);

        int rc = p.run(true, 1000);
        assertEquals("return code from external process,", 0, rc);
    }

    @Test
    public void testExeException() throws Exception {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(command(0, 0, true, false));
        p.setWorkingDirectory(exeDir);
        p.logStdOut(true);
        p.logStdErr(true);

        int rc = p.run(true, 1000);
        assertTrue(rc != 0);

    }

    @Test
    public void testExeFail() throws Exception {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(command(1, 0, false, false));
        p.setWorkingDirectory(exeDir);
        p.logStdOut(true);
        p.logStdErr(true);

        int rc = p.run(true, 1000);
        assertEquals(1, rc);

    }

    @Test
    public void testDifferentWorkingDir() throws Exception {

        CommandLine command = new CommandLine(exe.getCanonicalPath());
        command.addArgument("0"); // retcode
        command.addArgument("0"); // sleeptime
        command.addArgument("0"); // crash flag
        command.addArgument("1"); // touch flag

        ExternalProcess p = ExternalProcess.simpleExternalProcess(command);
        p.setWorkingDirectory(workingDir.getCanonicalFile());
        p.logStdOut(true);
        p.logStdErr(true);

        int rc = p.run(true, 1000);

        assertEquals("return code from external process,", 0, rc);
        File touchFile = new File(workingDir.getCanonicalFile(), "touch.txt");
        assertTrue(touchFile.exists());
    }

    @Test
    public void testOutputWriters() throws IOException {

        ExternalProcess p = new ExternalProcess(true, null, true, null);
        p.setWorkingDirectory(workingDir.getCanonicalFile());
        p.writeStdErr(true);
        p.writeStdOut(true);

        CommandLine commandLine = new CommandLine(exe.getCanonicalPath());
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        p.setCommandLine(commandLine);

        int rc = p.run(true, 1000);
        assertEquals(0, rc);
        String stdoutString = p.getStdoutString();
        String stderrString = p.getStderrString();
        assertNotNull(stdoutString);
        assertNotNull(stderrString);
        assertTrue(stdoutString.startsWith("USAGE:"));
        assertTrue(stderrString.startsWith("Here is some error stream content\n"));
    }

    @Test
    public void testExecute() throws IOException {
        ExternalProcess p = new ExternalProcess(true, null, true, null);
        p.setWorkingDirectory(workingDir.getCanonicalFile());
        p.writeStdErr(true);
        p.writeStdOut(true);

        CommandLine commandLine = new CommandLine(exe.getCanonicalPath());
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        p.setCommandLine(commandLine);

        int rc = p.execute();
        assertEquals(0, rc);
    }

    @Test
    public void testTimeout() throws IOException {
        ExternalProcess p = new ExternalProcess(true, null, true, null);
        p.setWorkingDirectory(workingDir.getCanonicalFile());
        p.writeStdErr(true);
        p.writeStdOut(true);

        CommandLine commandLine = new CommandLine(exe.getCanonicalPath());
        commandLine.addArgument("0");
        commandLine.addArgument("1000");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        p.setCommandLine(commandLine);
        p.timeout(100);

        int rc = p.execute();
        assertNotEquals(0, rc);
    }

    @Test
    public void testLogOutputStreamClasses() throws IOException {
        ExternalProcess p = new ExternalProcess(false, null, false, null);
        CommandLine commandLine = new CommandLine(exe.getCanonicalPath());
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        commandLine.addArgument("0");
        p.setCommandLine(commandLine);

        p.logStdOut(true);
        p.execute();
        assertEquals(PlainTextLogOutputStream.class, p.outputLogClass());
        assertNull(p.errorLogClass());

        p.logStdErr(true);
        p.execute();
        assertEquals(PlainTextLogOutputStream.class, p.outputLogClass());
        assertEquals(PlainTextLogOutputStream.class, p.errorLogClass());

        p.writeStdOut(true);
        p.execute();
        assertEquals(WriterLogOutputStream.class, p.outputLogClass());
        assertEquals(PlainTextLogOutputStream.class, p.errorLogClass());

        p.writeStdErr(true);
        p.execute();
        assertEquals(WriterLogOutputStream.class, p.outputLogClass());
        assertEquals(WriterLogOutputStream.class, p.errorLogClass());

        p.logStdOut(false);
        p.logStdErr(false);
        p.writeStdOut(false);
        p.writeStdErr(false);
        p.execute();
        assertNull(p.errorLogClass());
        assertNull(p.outputLogClass());

    }

    @Test(expected = PipelineException.class)
    public void testBadExeName() {
        File dir = new File("build/bin");
        File exe = new File(dir.getAbsoluteFile(), "testpro");

        CommandLine command = new CommandLine(exe.getAbsolutePath());
        command.addArgument("0"); // retcode
        command.addArgument("0"); // sleeptime
        command.addArgument("0"); // crash flag

        ExternalProcess p = ExternalProcess.simpleExternalProcess(command);
        p.setWorkingDirectory(dir.getAbsoluteFile());
        p.logStdOut(true);
        p.logStdErr(true);

        p.run(true, 1000);
    }

    @Test
    public void testExeInPath() {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(new CommandLine("ls"));
        assertEquals(0, p.execute());
    }

    @Test(expected = PipelineException.class)
    public void testExeNotInPath() {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(new CommandLine("lssssss"));
        assertEquals(0, p.execute());
    }
}
