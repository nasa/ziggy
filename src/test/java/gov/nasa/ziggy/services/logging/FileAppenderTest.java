package gov.nasa.ziggy.services.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.io.Filenames;

public class FileAppenderTest {

    private static final String LOG4J_CONFIG_FILE = "test/data/logging/log4j2.xml";
    private static Logger log;
    private File appenderFile;

    @Before
    public void setup() {
        appenderFile = new File("build/test", "file-appender.log");
        System.setProperty("log4j2.configurationFile", LOG4J_CONFIG_FILE);
        log = LoggerFactory.getLogger(FileAppenderTest.class);
    }

    @After
    public void teardown() throws IOException {
        System.clearProperty("log4j2.configurationFile");
        appenderFile.delete();
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test
    public void testFileAppender() throws IOException {

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration c = ctx.getConfiguration();
        LoggerConfig lc = c.getLoggerConfig("");
        lc.setLevel(Level.ALL);

        File appenderFile = new File("build/test", "file-appender.log");

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
