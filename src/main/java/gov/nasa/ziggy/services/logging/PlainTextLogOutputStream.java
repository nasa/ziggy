package gov.nasa.ziggy.services.logging;

import java.util.regex.Pattern;

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.SubtaskUtils;

/**
 * Subclass of {@link LogOutputStream} used in the context of a {@link PumpStreamHandler} to capture
 * stdout / stderr from a {@link DefaultExecutor} and transfer it to the log file of the thread that
 * runs the {@link DefaultExecutor}.
 * <p>
 * The arguments to {{@link #processLine(String, int)} may come from the output of a process running
 * in the {@link DefaultExecutor} that doesn't prepend a timestamp to the message, or it might be a
 * log message from a Java class running inside the {@link DefaultExecutor}, in which case there
 * will be a prepended timestamp. The {{@link #processLine(String, int)} checks for the timestamp at
 * the start of a line; if the timestamp is present, the line is written as-is to stdout, if the
 * timestamp is absent, the line is written to the logger for the {@link PlainTextLogOutputStream}
 * class. In this way it is guaranteed that all output has one and only one timestamp prepended.
 * <p>
 * As the messages started out as stdout or stderr from the {@link DefaultExecutor} process, there
 * is no level information available, so all messages are written at info level.
 *
 * @author PT
 */
public class PlainTextLogOutputStream extends LogOutputStream {

    private String logStreamIdentifier;

    public PlainTextLogOutputStream() {
        super();
    }

    public PlainTextLogOutputStream(String logStreamIdentifier) {
        super();
        this.logStreamIdentifier = logStreamIdentifier;
    }

    private static Logger log = LoggerFactory.getLogger(PlainTextLogOutputStream.class);

    private final Pattern LOG4J_DATE_PATTERN = Pattern
        .compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}");

    @Override
    protected void processLine(String line, int level) {

        // The output stream is used in the context of a DefaultExecutor, which means that
        // it potentially operates in a different thread than the one that constructed the
        // DefaultExecutor. Consequently it's necessary to set the appropriate thread
        // context map entry when writing logs. Unfortunately, it's not clear to me that
        // setting this once will be safe, so we set it every time a message is logged.
        if (logStreamIdentifier != null && !logStreamIdentifier.isEmpty()) {
            SubtaskUtils.putLogStreamIdentifier(logStreamIdentifier);
        }

        // If the datestamp at the beginning is missing, this message hasn't passed through
        // the logging infrastructure at any point, if so, put it through now

        if (line.length() < 23 || !LOG4J_DATE_PATTERN.matcher(line.substring(0, 23)).matches()) {

            // The output stream is used in the context of a DefaultExecutor, which means that
            // it potentially operates in a different thread than the one that constructed the
            // DefaultExecutor. Consequently it's necessary to set the appropriate thread
            // context map entry when writing logs. Unfortunately, it's not clear to me that
            // setting this once will be safe, so we set it every time a message is logged.
            if (logStreamIdentifier != null && !logStreamIdentifier.isEmpty()) {
                SubtaskUtils.putLogStreamIdentifier(logStreamIdentifier);
            }

            log.info(line);
        } else {
            System.out.println(line);
        }
    }
}
