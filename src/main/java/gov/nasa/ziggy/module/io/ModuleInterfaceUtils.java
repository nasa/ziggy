package gov.nasa.ziggy.module.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.FileUtil;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.annotation.XmlRootElement;

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

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void writeCompanionXmlFile(Persistable inputs, String moduleName) {
        if (!inputs.getClass().isAnnotationPresent(XmlRootElement.class)) {
            return;
        }
        String companionXmlFile = xmlFileName(moduleName);
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

            try (
                Writer fileWriter = new OutputStreamWriter(new FileOutputStream(companionXmlFile),
                    FileUtil.ZIGGY_CHARSET);
                BufferedWriter bufWriter = new BufferedWriter(fileWriter)) {
                marshaller.marshal(inputs, bufWriter);
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to write to file " + companionXmlFile.toString(), e);
            }
            if (validationErrors.length() > 0) {
                throw new PipelineException(validationErrors.toString());
            }
        } catch (JAXBException e) {
            throw new PipelineException(validationErrors.toString(), e);
        }
    }

    public static AlgorithmErrorReturn dumpErrorFile(File errorFile, String moduleName) {

        return dumpErrorFile(errorFile, true);
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public static AlgorithmErrorReturn dumpErrorFile(File errorFile, boolean logit) {
        AlgorithmErrorReturn errorReturn = new AlgorithmErrorReturn();
        String errorMessage;
        try {
            Hdf5ModuleInterface hdf5Interface = new Hdf5ModuleInterface();
            hdf5Interface.readFile(errorFile, errorReturn, true);

            errorReturn.logStackTrace();

            errorMessage = "MATLAB code generated an error file, message = "
                + errorReturn.getMessage();
        } catch (Exception e) {
            // We want execution to continue regardless of what runtime or checked
            // exception might have been thrown in the preceding, which is why
            // Exception is caught.
            errorMessage = "MATLAB code generated an error file, but it was unreadable ("
                + e.getMessage() + ")";
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
     */
    public static void clearStaleErrorState(File dataDir, String filenamePrefix) {
        File currentErrorFile = errorFile(dataDir, filenamePrefix);

        if (currentErrorFile.exists()) {
            deleteErrorFile(currentErrorFile);
        }
    }

    public static File errorFile(File dataDir, String filenamePrefix) {
        return new File(dataDir, errorFileName(filenamePrefix));
    }

    private static void deleteErrorFile(File errorFileToDelete) {
        boolean deleted = errorFileToDelete.delete();
        if (!deleted) {
            log.error("Failed to delete errorFile=" + errorFileToDelete);
        }
    }

    /**
     * Returns the name of the task inputs file for a given module. This is used only in the case
     * where a partially-populated inputs file is to be written into the task directory.
     */
    public static String inputsFileName(String moduleName) {
        return moduleName + "-inputs." + BIN_FILE_TYPE;
    }

    /**
     * Returns the name of the sub-task outputs file for a given module and sequence number.
     */
    public static String outputsFileName(String moduleName) {
        return moduleName + "-outputs." + BIN_FILE_TYPE;
    }

    /**
     * Returns a pattern that can be used to match filenames to see whether they are pipeline output
     * files.
     */
    public static Pattern outputsFileNamePattern(String moduleName) {
        String regex = moduleName + "-outputs." + BIN_FILE_TYPE;
        return Pattern.compile(regex);
    }

    /**
     * Returns the name of the sub-task error file for a given module and sequence number.
     */
    public static String errorFileName(String moduleName) {
        return moduleName + "-error." + BIN_FILE_TYPE;
    }

    /**
     * Returns the name of the sub-task XML companion file for a given module and sequence number.
     */
    public static String xmlFileName(String moduleName) {
        return moduleName + "-digest.xml";
    }

    /**
     * Returns the name of the file for capture of stdout from a task.
     */
    public static String stdoutFileName(String moduleName) {
        return moduleName + "-stdout.log";
    }

    /**
     * Returns the name of the file for capture of stderr from a task.
     */
    public static String stderrFileName(String moduleName) {
        return moduleName + "-stdout.log";
    }
}
