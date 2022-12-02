package gov.nasa.ziggy.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for dealing with exceptions. For exception utilities for dealing with functional
 * programming see ExceptionConversions.java.
 *
 * @author Sean McCauliff
 */
public class ThrowableUtils {

    private static final Logger log = LoggerFactory.getLogger(ThrowableUtils.class);

    /**
     * Writes the stack trace to a file. This method is sometimes calls from the debugger evaluate
     * window and so there may not be a reference to in in the code.
     *
     * @param fname The full path to the file.
     * @param t
     */
    public static void stackTraceToFile(String fname, Throwable t) {
        try (FileWriter fw = new FileWriter(fname);
            BufferedWriter bufferedWriter = new BufferedWriter(fw);
            PrintWriter printWriter = new PrintWriter(bufferedWriter)) {
            t.printStackTrace(printWriter);
        } catch (OutOfMemoryError oome) {
            throw oome;
        } catch (Throwable innerT) {
            log.error("Throwable while trying to dump exception to file.");
        }
    }
}
