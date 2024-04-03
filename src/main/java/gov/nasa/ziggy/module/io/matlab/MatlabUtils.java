package gov.nasa.ziggy.module.io.matlab;

import java.io.File;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * Supplies utility functions used for configuring pipelines for MATLAB execution.
 *
 * @author PT
 */
public class MatlabUtils {

    // Never try to instantiate this class.
    private MatlabUtils() {
    }

    /**
     * Returns the library paths needed for the MATLAB Compiler Runtime (MCR) based on the location
     * of the MCR directory itself.
     *
     * @param mcrRoot Location of the MCR directory. Note that this is different from mcr_root as it
     * is used in Mathworks documentation in that mcrRoot includes the version portion of the path,
     * i.e., mcrRoot = Mathworks' mcrRoot/version .
     * @return A string of appropriate library paths for deployed MATLAB applications, based on the
     * OS type and location of the MCR root.
     */
    public static String mcrPaths(String mcrRoot) {
        String suffix = null;
        String openGlString = null;
        switch (osType()) {
            case DEFAULT:
            case LINUX:
                suffix = "glnxa64";
                openGlString = "opengl" + File.separator + "lib";
                break;
            case MAC_OS_X:
                if (architecture().contains("aarch")) {
                    suffix = "maca64";
                } else {
                    suffix = "maci64";
                }
                openGlString = "";
                break;
            default:
                throw new PipelineException("OS type " + osType().toString() + " not supported");
        }
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append(
            mcrRoot + File.separator + "runtime" + File.separator + suffix + File.pathSeparator);
        pathBuilder.append(
            mcrRoot + File.separator + "bin" + File.separator + suffix + File.pathSeparator);
        pathBuilder.append(
            mcrRoot + File.separator + "sys" + File.separator + "os" + File.separator + suffix);
        if (!openGlString.isEmpty()) {
            pathBuilder.append(File.pathSeparator + mcrRoot + File.separator + "sys"
                + File.separator + "opengl" + File.separator + "lib" + File.separator + suffix);
        }
        return pathBuilder.toString();
    }

    private static OperatingSystemType osType() {
        return OperatingSystemType.getInstance();
    }

    private static String architecture() {
        return ZiggyConfiguration.getInstance()
            .getString(PropertyName.ARCHITECTURE.property())
            .toLowerCase();
    }
}
