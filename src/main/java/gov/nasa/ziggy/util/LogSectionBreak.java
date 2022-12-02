package gov.nasa.ziggy.util;

import org.slf4j.Logger;

public class LogSectionBreak {

    private LogSectionBreak() {
    }

    /**
     * Writes a visible section break into a log file
     *
     * @param log the log file which needs the section break
     * @param message a message, if any, which is to be written to the middle of the break
     */
    public static void writeSectionBreak(Logger log, String message) {
        String breakString = "//=========================================================";
        log.info("");
        log.info(breakString);
        log.info("//");
        log.info("//");
        log.info("//         " + message);
        log.info("//");
        log.info("//");
        log.info(breakString);
        log.info("");
    }
}
