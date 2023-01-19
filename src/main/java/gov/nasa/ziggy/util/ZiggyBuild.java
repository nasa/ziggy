package gov.nasa.ziggy.util;

import java.text.SimpleDateFormat;

import org.slf4j.Logger;

/**
 * Provides information gathered from the build environment.
 *
 * @author Miles Cote
 */
public class ZiggyBuild {
    private static final SimpleDateFormat READABLE_LOCAL_FORMAT = new SimpleDateFormat(
        "yyyy-MM-dd--HH.mm.ss");

    public static final String getId() {
        synchronized (READABLE_LOCAL_FORMAT) {
            return ZiggyVersion.getSoftwareVersion() + "--"
                + READABLE_LOCAL_FORMAT.format(ZiggyVersion.getBuildDate());
        }
    }

    public static void logVersionInfo(Logger log) {
        log.info("Software Version: " + ZiggyVersion.getSoftwareVersion());
        log.info("  Branch: " + ZiggyVersion.getBranch());
        log.info("  Revision: " + ZiggyVersion.getRevision());
        log.info("  Build Date: " + ZiggyVersion.getBuildDate());
    }

}
