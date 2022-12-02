package gov.nasa.ziggy.util.io;

/**
 * Constants used for standard file and directory names.
 * <p>
 * Directories end with a separator character so that one can simply say, for example, ETC +
 * LOG4J_CONFIG using static imports.
 *
 * @author Forrest Girouard
 * @author Bill Wohler
 */
public class Filenames {
    /**
     * The directory path containing transient unit test products.
     */
    public static final String BUILD_TEST = "build/test";

    /**
     * Directory used to store temporary artifacts. The value is {@code
     * build/tmp} instead of {@code tmp} so there is plenty of room for large fits and bin files.
     */
    public static final String BUILD_TMP = "build/tmp";

    /** The default name of the log4j configuration file (log4j.xml). */
    public static final String LOG4J_CONFIG = "log4j.xml";

    private Filenames() {
    }
}
