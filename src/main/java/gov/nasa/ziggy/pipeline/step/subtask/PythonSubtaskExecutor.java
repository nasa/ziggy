package gov.nasa.ziggy.pipeline.step.subtask;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.step.FatalAlgorithmProcessingException;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * A {@link SubtaskExecutor} that provides additional support for Python algorithms.
 * <p>
 * For Python execution, the step run by Ziggy is a shell script, build/bin/ziggy-python . This
 * script activates the Python virtual environment and invokes a Python function. The Python
 * function runs the user's code within a try-catch block that generates the stack trace file in the
 * event of a failure in the Python code.
 * <p>
 * The shell script needs to know which environment to activate, which Python module to run, and
 * which function within the module to execute. All of this information is provided via environment
 * variables that are instantiated as part of the {@link ExternalProcess} environment. The Python
 * module and package are obtained from the binary name of the given step, while the Python function
 * to run is the pipeline step name.
 *
 * @author PT
 */
public class PythonSubtaskExecutor extends SubtaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(PythonSubtaskExecutor.class);

    // Name of the shell script that wraps all Python code.
    static final String PYTHON_BINARY = "ziggy-python";

    // Name of the environment variable where the virtual environment path will be stored.
    static final String VIRTUAL_ENVIRONMENT_ENV_VAR_NAME = "ZIGGY_VIRT_ENV";

    // Name of the environment variable where the python module name is stashed.
    static final String PYTHON_MODULE_ENV_VAR_NAME = "ZIGGY_PYTHON_MODULE";

    // Name of the environment variable where the python function name is stashed.
    static final String PYTHON_FUNCTION_ENV_VAR_NAME = "ZIGGY_PYTHON_FUNCTION";

    // Name for a default environment to be used if no other environment is found.
    static final String PYTHON_DEFAULT_VENV_NAME = "pipeline";

    // Character in the Python binary name that denotes package/module separation.
    // Note that this is not necessarily the file separator character.
    static final String PYTHON_MODULE_PATH_SEPARATOR = "/";

    // Character Python uses to indicate subpackages or modules within a package.
    static final String PYTHON_PACKAGE_MODULE_SEPARATOR = ".";

    private String pythonModuleName;
    private Path virtualEnvironmentPath;

    PythonSubtaskExecutor(File taskDir, int subtaskIndex, String binaryName, int timeoutSecs) {
        super(taskDir, subtaskIndex, PYTHON_BINARY, timeoutSecs);
        pythonModuleName = pythonModuleName(binaryName);
        log.info("Python module: {}", pythonModuleName);
        virtualEnvironmentPath = virtualEnvironmentPath(pythonModuleName);
        if (virtualEnvironmentPath != null) {
            log.info("Python virtual environment: {}", virtualEnvironmentPath.toString());
        } else {
            log.info("No Python virtual environment specified");
        }
    }

    // Constructs the Python module name. Specifically, a path to the Python file, like
    // "foo/bar/baz.py" is converted to "foo.bar.baz", which tells Python that the baz
    // Python module in the bar subpackage of the foo package is the file to use.
    private String pythonModuleName(String binaryName) {
        int suffixLocation = binaryName.indexOf(SubtaskExecutor.PYTHON_SUFFIX);
        if (suffixLocation < 0) {
            throw new FatalAlgorithmProcessingException(
                "File " + binaryName + " lacks Python suffix " + SubtaskExecutor.PYTHON_SUFFIX);
        }
        String strippedBinaryName = binaryName.substring(0, suffixLocation);

        // Replace path separator characters with dots to turn the file name and location
        // into a module name, potentially one within a package.
        return strippedBinaryName.replace(PYTHON_MODULE_PATH_SEPARATOR,
            PYTHON_PACKAGE_MODULE_SEPARATOR);
    }

    // Locates the virtual environment, if any, that should be activated for this pipeline
    // step. The precedence order is as follows:
    // build/env/<pipeline-step-name> if it exists, otherwise,
    // build/env/<python-module-name> if it exists, otherwise,
    // build/env/pipeline if it exists, otherwise,
    // build/env if it exists, otherwise,
    // null.
    private Path virtualEnvironmentPath(String pythonModuleName) {

        // No environment directory implies no environment to activate.
        if (!directoryExists(DirectoryProperties.pythonEnvDir())) {
            return null;
        }

        // Look for an environment that has the pipeline step name.
        if (directoryExists(DirectoryProperties.pythonEnvDir().resolve(pipelineStepName()))) {
            return DirectoryProperties.pythonEnvDir().resolve(pipelineStepName());
        }

        // Look for an environment that has the Python module name.
        String strippedModuleName = pythonModuleName
            .substring(pythonModuleName.lastIndexOf(PYTHON_PACKAGE_MODULE_SEPARATOR) + 1);
        if (directoryExists(DirectoryProperties.pythonEnvDir().resolve(strippedModuleName))) {
            return DirectoryProperties.pythonEnvDir().resolve(strippedModuleName);
        }

        // Look for a default environment.
        if (directoryExists(DirectoryProperties.pythonEnvDir().resolve(PYTHON_DEFAULT_VENV_NAME))) {
            return DirectoryProperties.pythonEnvDir().resolve(PYTHON_DEFAULT_VENV_NAME);
        }

        // If none of the above was found, return the env directory itself.
        return DirectoryProperties.pythonEnvDir();
    }

    private boolean directoryExists(Path directory) {
        boolean exists = Files.exists(directory);
        boolean isDirectory = Files.isDirectory(directory);
        return exists && isDirectory;
    }

    // Adds envrironment variables for Python resources needed at runtime. These
    // environment variables will be consumed by the ziggy-python shell script
    // and used to activate the correct environment and launch the correct function.
    @Override
    protected Map<String, String> customizeEnvironment() {

        Map<String, String> environment = new HashMap<>();
        // Add the python module name.
        environment.put(PYTHON_MODULE_ENV_VAR_NAME, pythonModuleName);

        // Add the name of the Python function to be invoked.
        environment.put(PYTHON_FUNCTION_ENV_VAR_NAME, pipelineStepName());

        // If there's a virtual environment, add it as an environment variable.
        if (virtualEnvironmentPath != null) {
            environment.put(VIRTUAL_ENVIRONMENT_ENV_VAR_NAME, virtualEnvironmentPath.toString());
        }
        return environment;
    }
}
