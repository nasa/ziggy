package gov.nasa.ziggy.services.logging;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

public class FileAppenderTest {

    private static final String LOG4J_CONFIG_FILE = TEST_DATA.resolve("logging")
        .resolve("log4j2.xml")
        .toString();

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule log4j2ConfigurationFilePropertyRule = new ZiggyPropertyRule(
        PropertyName.LOG4J2_CONFIGURATION_FILE, LOG4J_CONFIG_FILE);

    @Test
    public void testFileAppender() throws IOException {

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration c = ctx.getConfiguration();
        LoggerConfig lc = c.getLoggerConfig("");
        lc.setLevel(Level.ALL);

        Path appenderFile = directoryRule.directory().resolve("file-appender.log");

        FileAppender taskLog = FileAppender.newBuilder()
            .withFileName(appenderFile.toString())
            .setName("file-appender.log")
            .build();
        taskLog.start();
        lc.addAppender(taskLog, Level.ALL, null);

        Logger log = LoggerFactory.getLogger(FileAppenderTest.class);
        log.info("Here is a test message with INFO level");
        log.warn("Here is a test message with WARN level");

        lc.removeAppender("file-appender.log");

        assertTrue(Files.exists(appenderFile));

        List<String> logLines = Files.readAllLines(appenderFile, ZiggyFileUtils.ZIGGY_CHARSET);
        assertEquals(2, logLines.size());
        assertTrue(logLines.get(0).contains("Here is a test message with INFO level"));
        assertTrue(logLines.get(1).contains("Here is a test message with WARN level"));
    }
}
