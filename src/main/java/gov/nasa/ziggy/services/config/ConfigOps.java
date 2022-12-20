package gov.nasa.ziggy.services.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.configuration.Configuration;

import gov.nasa.ziggy.module.PipelineException;

/**
 * Some additional operations over a Configuration object.
 *
 * @author Todd Klaus
 */
public class ConfigOps {

    /**
     * Gets a directory property from the ConfigurationService as a File without creating the
     * directory. This is equivelent to getDirectory(propName, false)
     *
     * @param propName a property name
     * @return non-null
     */
    public static File getDirectory(String propName) {
        return ConfigOps.getDirectory(propName, false);
    }

    /**
     * Gets a directory property from the ConfigurationService as a File optionally creating it.
     *
     * @param propName a property name
     * @param makeDirIfNonexistent When true and the value of this property does not exist as a
     * directory then one is created with mkdirs().
     * @return non-null
     */
    public static File getDirectory(String propName, boolean makeDirIfNonexistent) {

        Configuration configuration = ZiggyConfiguration.getInstance();
        if (!configuration.containsKey(propName)) {
            throw new PipelineException("Required config property not found: " + propName);
        }

        String dirName = configuration.getString(propName);
        File dir = new File(dirName);
        if (!dir.exists()) {
            if (!makeDirIfNonexistent) {
                throw new PipelineException("Directory  " + dirName
                    + " does not exist as specified in property " + propName + ".");
            }
            try {
                Files.createDirectories(dir.toPath());
            } catch (IOException e) {
                throw new PipelineException("Failed to create direcotry " + dirName
                    + " specified in property " + propName + ".", e);
            }
        }
        if (!dir.isDirectory()) {
            throw new PipelineException("Directory exists, but is not a directory: " + dirName
                + " specified in property " + propName + ".");
        }

        return dir;
    }
}
