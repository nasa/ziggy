package gov.nasa.ziggy.module.remote;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * This class logs all qsub command lines to a file for debugging purposes. The log file only
 * contains the qsub command line, so it can be run as a shell script if desired.
 *
 * @author todd klaus
 */
public class QsubLog {
    @SuppressWarnings("unused")
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

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void log(String qsubCommandLine) {
        try (BufferedWriter bw = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(logFile, true), ZiggyFileUtils.ZIGGY_CHARSET))) {
            // append to existing file if it exists
            bw.write(qsubCommandLine + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to write to file " + logFile.toString(), e);
        }
    }
}
