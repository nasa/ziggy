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
     * Directory used to store temporary artifacts. The value is {@code
     * build/tmp} instead of {@code tmp} so there is plenty of room for large fits and bin files.
     */
    public static final String BUILD_TMP = "build/tmp";

    private Filenames() {
    }
}
