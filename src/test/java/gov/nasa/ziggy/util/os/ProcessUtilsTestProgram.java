package gov.nasa.ziggy.util.os;

/**
 * This is used to test executing a Java class in a different process.
 *
 * @author Sean McCauliff
 */
public class ProcessUtilsTestProgram {
    /**
     * @param args
     */
    public static void main(String[] argv) {
        int exitStatus = Integer.parseInt(argv[0]);
        System.out.println(argv[1]);
        System.exit(exitStatus);
    }
}
