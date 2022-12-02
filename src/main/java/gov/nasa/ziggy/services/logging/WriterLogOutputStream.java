package gov.nasa.ziggy.services.logging;

import java.io.IOException;
import java.io.Writer;

import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.output.WriterOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.SubtaskUtils;

/**
 * Subclass of {@link LogOutputStream} that is used when messages need to be sent to both the
 * algorithm log file and a text file (for example, the stdout file from the algorithm, in the
 * working directory). A {@link WriterOutputStream} is used in the context of a
 * {@link PumpStreamHandler}, which means that messages received at
 * {{@link #processLine(String, int)} will arrive without meaningful log-level information. For this
 * reason, they are written to the algorithm log as level INFO.
 *
 * @author PT
 * @author Todd Klaus
 */
public class WriterLogOutputStream extends LogOutputStream {
    private static final Logger log = LoggerFactory.getLogger(WriterLogOutputStream.class);

    private final Writer writer;
    private boolean logOutput = false;
    private String logStreamIdentifier;
    private StringBuilder stringBuilder = new StringBuilder();

    public WriterLogOutputStream(Writer writer) {
        this.writer = writer;
    }

    public WriterLogOutputStream(Writer writer, boolean logOutput) {
        this.writer = writer;
        this.logOutput = logOutput;
    }

    public WriterLogOutputStream(Writer writer, boolean logOutput, String logStreamIdentifier) {
        this.writer = writer;
        this.logOutput = logOutput;
        this.logStreamIdentifier = logStreamIdentifier;
    }

    @Override
    protected void processLine(String line, int level) {
        try {
            writer.write(line);
            writer.write("\n");
            stringBuilder.append(line);
            stringBuilder.append("\n");
        } catch (IOException e) {
            log.info("Unable to write log output, caught e = " + e, e);
        }

        if (logOutput) {

            // The output stream is used in the context of a DefaultExecutor, which means that
            // it potentially operates in a different thread than the one that constructed the
            // DefaultExecutor. Consequently it's necessary to set the appropriate thread
            // context map entry when writing logs. Unfortunately, it's not clear to me that
            // setting this once will be safe, so we set it every time a message is logged.
            if (logStreamIdentifier != null && !logStreamIdentifier.isEmpty()) {
                SubtaskUtils.putLogStreamIdentifier(logStreamIdentifier);
            }

            log.info(line);
        }
    }

    @Override
    public String toString() {
        return stringBuilder.toString();
    }
}
