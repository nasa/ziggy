package gov.nasa.ziggy.services.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ZiggyPropertyRule;

public class FileAppenderTest {

    private static final String LOG4J_CONFIG_FILE = "test/data/logging/log4j2.xml";
    private static Logger log;
    private File appenderFile;
    private Path testClassDir;

    @Rule
    public ZiggyPropertyRule log4j2ConfigurationFilePropertyRule = new ZiggyPropertyRule(
        "log4j2.configurationFile", LOG4J_CONFIG_FILE);

    // Important note:
    // For some reason, when this class is refactored to use the ZiggyDirectoryRule, the
    // test fails. However, when the content of the ZiggyDirectoryRule is implemented in
    // the class and its methods as discrete statements, the test passes! For now, we've
    // decided to live with this mystery.
    @Before
    public void setup() throws IOException {
        testClassDir = Paths.get("build", "test", "services.logging.FileAppenderTest");
        Files.createDirectories(testClassDir);
        log = LoggerFactory.getLogger(FileAppenderTest.class);
    }

    @Test
    public void testFileAppender() throws IOException {

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration c = ctx.getConfiguration();
        LoggerConfig lc = c.getLoggerConfig("");
        lc.setLevel(Level.ALL);

        Path testDir = testClassDir.resolve("testFileAppender");
        Files.createDirectories(testDir);
        appenderFile = testDir.resolve("file-appender.log").toFile();
        if (appenderFile.exists()) {
            appenderFile.delete();
        }

        FileAppender taskLog = FileAppender.newBuilder()
            .withFileName(appenderFile.getAbsolutePath())
            .setName("file-appender.log")
            .build();
        taskLog.start();
        lc.addAppender(taskLog, Level.ALL, null);

        log.info("Here is a test message with INFO level");
        log.warn("Here is a test message with WARN level");

        lc.removeAppender("file-appender.log");

        assertTrue(appenderFile.exists());

        List<String> logLines = FileUtils.readLines(appenderFile, (Charset) null);
        assertEquals(2, logLines.size());
        assertTrue(logLines.get(0).contains("Here is a test message with INFO level"));
        assertTrue(logLines.get(1).contains("Here is a test message with WARN level"));

    }

}
