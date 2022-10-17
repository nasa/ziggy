package gov.nasa.ziggy.module.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.matlab.MatlabErrorReturn;

/**
 * Utilities for use in the interface between Ziggy and the processing modules. These tools relate
 * to the input, output, and error files, plus the (optional) XML "digest" of a UOW.
 *
 * @author Todd Klaus
 */
public class ModuleInterfaceUtils {
    private static final Logger log = LoggerFactory.getLogger(ModuleInterfaceUtils.class);

    static final String BIN_FILE_TYPE = "h5";

    private ModuleInterfaceUtils() {
    }

    public static void writeCompanionXmlFile(Persistable inputs, String moduleName, int seqNum) {
        if (!inputs.getClass().isAnnotationPresent(XmlRootElement.class)) {
            return;
        }
        String companionXmlFile = xmlFileName(moduleName, seqNum);
        log.info("Writing companion xml file \"" + companionXmlFile + "\".");
        StringBuilder validationErrors = new StringBuilder();
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(inputs.getClass());
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            marshaller.setEventHandler(event -> {
                if (event.getLinkedException().getClass() == OutOfMemoryError.class) {
                    event.getLinkedException().printStackTrace();
                }
                validationErrors.append(event).append('\n');
                return true; // continue what ever is going on.
            });

            try (FileWriter fileWriter = new FileWriter(companionXmlFile);
                BufferedWriter bufWriter = new BufferedWriter(fileWriter)) {
                marshaller.marshal(inputs, bufWriter);
            }
            if (validationErrors.length() > 0) {
                throw new PipelineException(validationErrors.toString());
            }
        } catch (Exception e) {
            throw new PipelineException(validationErrors.toString(), e);
        }
    }

    public static MatlabErrorReturn dumpErrorFile(File errorFile, String moduleName) {

        return dumpErrorFile(errorFile, true);
    }

    public static MatlabErrorReturn dumpErrorFile(File errorFile, boolean logit) {
        MatlabErrorReturn errorReturn = new MatlabErrorReturn();
        String errorMessage;
        try {
            Hdf5ModuleInterface hdf5Interface = new Hdf5ModuleInterface();
            hdf5Interface.readFile(errorFile, errorReturn, true);

            errorReturn.logStackTrace();

            errorMessage = "MATLAB code generated an error file, message = "
                + errorReturn.getMessage();
        } catch (Throwable t) {
            errorMessage = "MATLAB code generated an error file, but it was unreadable ("
                + t.getMessage() + ")";
            errorReturn.setMessage(errorMessage);
        }
        if (logit) {
            log.warn(errorMessage);
        }

        return errorReturn;
    }

    /**
     * Make sure there are no leftover error files before launching the process. If a MATLAB process
     * generates an error, but the error is not picked up by the Java process (because it crashed,
     * etc.), then the stale error file may still exist when the task is restarted later. When that
     * restarted task finishes, the Java process will see the stale error file and think that the
     * new process failed, when in fact it was the old (previous) process. We mitigate that by
     * deleting any existing error files before launching the process.
     *
     * @param dataDir
     * @param filenamePrefix
     * @param seqNum
     */
    public static void clearStaleErrorState(File dataDir, String filenamePrefix, int seqNum) {
        File currentErrorFile = errorFile(dataDir, filenamePrefix, seqNum);

        if (currentErrorFile.exists()) {
            deleteErrorFile(currentErrorFile);
        }
    }

    public static File errorFile(File dataDir, String filenamePrefix, int seqNum) {
        File errorFile = new File(dataDir, errorFileName(filenamePrefix, seqNum));
        return errorFile;
    }

    public static File errorFile(File dataDir, String filenamePrefix) {
        return errorFile(dataDir, filenamePrefix, 0);
    }

    private static void deleteErrorFile(File errorFileToDelete) {
        boolean deleted = errorFileToDelete.delete();
        if (!deleted) {
            log.error("Failed to delete errorFile=" + errorFileToDelete);
        }
    }

    /**
     * Returns the name of the sub-task inputs file for a given module and sequence number.
     *
     * @param moduleName
     * @param seqNum
     * @return
     */
    public static String inputsFileName(String moduleName, int seqNum) {
        return moduleName + "-inputs-" + seqNum + "." + BIN_FILE_TYPE;
    }

    /**
     * Returns the name of the task inputs file for a given module. This is used only in the case
     * where a partially-populated inputs file is to be written into the task directory.
     *
     * @param moduleName
     * @return
     */
    public static String inputsFileName(String moduleName) {
        return moduleName + "-inputs." + BIN_FILE_TYPE;
    }

    /**
     * Returns the name of the sub-task outputs file for a given module and sequence number.
     *
     * @param moduleName
     * @param seqNum
     * @return
     */
    public static String outputsFileName(String moduleName, int seqNum) {
        return moduleName + "-outputs-" + seqNum + "." + BIN_FILE_TYPE;
    }

    /**
     * Returns a pattern that can be used to match filenames to see whether they are pipeline output
     * files.
     *
     * @param moduleName
     * @return
     */
    public static Pattern outputsFileNamePattern(String moduleName) {
        String regex = moduleName + "-outputs-\\d+." + BIN_FILE_TYPE;
        return Pattern.compile(regex);
    }

    /**
     * Returns the name of the sub-task error file for a given module and sequence number.
     *
     * @param moduleName
     * @param seqNum
     * @return
     */
    public static String errorFileName(String moduleName, int seqNum) {
        return moduleName + "-error-" + seqNum + "." + BIN_FILE_TYPE;
    }

    /**
     * Returns the name of the sub-task XML companion file for a given module and sequence number.
     *
     * @param moduleName
     * @param seqNum
     * @return
     */
    public static String xmlFileName(String moduleName, int seqNum) {
        return moduleName + "-digest-" + seqNum + ".xml";
    }

    /**
     * Returns the name of the file for capture of stdout from a task.
     *
     * @param moduleName
     * @param logSuffix
     * @return
     */
    public static String stdoutFileName(String moduleName, String logSuffix) {
        return moduleName + "-stdout-" + logSuffix + ".log";
    }

    /**
     * Returns the name of the file for capture of stderr from a task.
     *
     * @param moduleName
     * @param logSuffix
     * @return
     */
    public static String stderrFileName(String moduleName, String logSuffix) {
        return moduleName + "-stdout-" + logSuffix + ".log";
    }
}
