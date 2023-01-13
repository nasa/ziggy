package gov.nasa.ziggy.ui.collections;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.util.io.FileUtil;

/**
 * @author Todd Klaus
 */
public class ArrayImportExportUtils {
    private static final Logger log = LoggerFactory.getLogger(ArrayImportExportUtils.class);

    /** Singleton pattern, all methods are static */
    private ArrayImportExportUtils() {
    }

    /**
     * Read an array from a text file, one element per line.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static List<String> importArray(File file) throws IOException {
        List<String> values = new LinkedList<>();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), FileUtil.ZIGGY_CHARSET));
            log.info("Importing array from: " + file.getName());

            String oneLine = null;

            do {
                oneLine = reader.readLine();

                if (oneLine != null && !oneLine.startsWith("#") && oneLine.length() > 0) {
                    values.add(oneLine);
                }
            } while (oneLine != null);
        } finally {
            FileUtil.close(reader);
        }

        return values;
    }

    public static void exportArray(File file, List<String> values) throws IOException {
        log.info("Exporting array to: " + file.getName());

        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(file), FileUtil.ZIGGY_CHARSET));) {
            writer.write("# Exported by ParametersUtils");
            writer.newLine();
            writer.write("#" + new Date().toString());
            writer.newLine();

            for (String value : values) {
                writer.write(value);
                writer.newLine();
            }
        }
    }
}
