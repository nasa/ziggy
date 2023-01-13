package gov.nasa.ziggy.module.remote;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.io.FileUtil;

/**
 * This class logs all qsub command lines to a file for debugging purposes. The log file only
 * contains the qsub command line, so it can be run as a shell script if desired.
 *
 * @author todd klaus
 */
public class QsubLog {
    private static final Logger log = LoggerFactory.getLogger(QsubLog.class);
    private static final String LOG_FILENAME = "qsub.log";

    File logFile = null;

    public QsubLog(File logDirectory) {
        if (!logDirectory.exists()) {
            throw new IllegalStateException("logDirectory does not exist: " + logDirectory);
        }

        if (!logDirectory.isDirectory()) {
            throw new IllegalStateException(
                "logDirectory exists, but is not a directory: " + logDirectory);
        }

        logFile = new File(logDirectory, LOG_FILENAME);
    }

    public void log(String qsubCommandLine) {
        try (BufferedWriter bw = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(logFile, true), FileUtil.ZIGGY_CHARSET))) {
            // append to existing file if it exists
            bw.write(qsubCommandLine + "\n");
        } catch (Exception e) {
            log.warn("Failed to write to qsub log file, caught: " + e, e);
        }
    }
}
