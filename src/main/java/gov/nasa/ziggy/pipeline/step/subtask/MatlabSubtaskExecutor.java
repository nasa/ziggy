package gov.nasa.ziggy.pipeline.step.subtask;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.ImmutableConfiguration;

import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * A {@link SubtaskExecutor} that provides additional support for deployed MATLAB algorithms. The
 * run command for the {@link ExternalProcess} is the same as for the base class, but the
 * environment setup requires some additional steps.
 *
 * @author PT
 */
public class MatlabSubtaskExecutor extends SubtaskExecutor {

    private static final String LM_LICENSE_FILE_ENV_NAME = "LM_LICENSE_FILE";
    private static final String MCR_CACHE_ROOT_ENV_VAR_NAME = "MCR_CACHE_ROOT";

    MatlabSubtaskExecutor(File taskDir, int subtaskIndex, String binaryName, int timeoutSecs,
        float heapSizeGigabytes, TaskConfiguration taskConfiguration) {
        super(taskDir, subtaskIndex, binaryName, timeoutSecs, heapSizeGigabytes, taskConfiguration);
    }

    @Override
    protected Map<String, String> customizeEnvironment() {
        Map<String, String> environment = new HashMap<>();
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        String mcrRoot = config.getString(PropertyName.MCRROOT.property(), null);

        // Create an MCR cache directory if it doesn't already exist.
        if (mcrRoot != null) {
            environment.put(MCR_CACHE_ROOT_ENV_VAR_NAME,
                PipelineExecutor.mcrCacheDir(pipelineStepName()).toString());
            try {
                if (!Files.exists(PipelineExecutor.mcrCacheDir(pipelineStepName()))) {
                    Files.createDirectories(PipelineExecutor.mcrCacheDir(pipelineStepName()));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            environment.put(MCR_CACHE_ROOT_ENV_VAR_NAME, "");
        }

        // Make sure LM_LICENSE_FILE is set to /dev/null since it otherwise
        // may cause undesirable access to the MATLAB license server at run time
        environment.put(LM_LICENSE_FILE_ENV_NAME, "/dev/null");

        // Override the definition of the library path to include the MCR runtime paths.
        String fullLibPath = libPath() + (!libPath().isBlank() ? File.pathSeparator : "")
            + MatlabUtils.mcrPaths(mcrRoot);
        environment.put(OperatingSystemType.newInstance().getSharedObjectPathEnvVar(), fullLibPath);
        return environment;
    }
}
