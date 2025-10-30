package gov.nasa.ziggy.pipeline.step;

import java.io.File;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.hdf5.Hdf5AlgorithmInterface;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputs;
import gov.nasa.ziggy.pipeline.step.io.PipelineOutputs;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.Persistable;
import gov.nasa.ziggy.util.io.ProxyIgnore;

/**
 * Serializes and deserializes the subtask count, inputs class, and outputs class for a given
 * {@link PipelineTask}.
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskConfiguration implements Persistable {
    private static final Logger log = LoggerFactory.getLogger(TaskConfiguration.class);
    private static final String PERSISTED_FILE_NAME = ".task-configuration.h5";
    public static final String LOCK_FILE_NAME = ".lock";

    @ProxyIgnore
    private File taskDir = null;

    private String inputsClassName;
    private String outputsClassName;
    private int subtaskCount;
    private float heapSizeGigabytes;
    private int activeCores;
    private int requestedTimeSeconds;
    private String executableName;

    public TaskConfiguration() {
    }

    public TaskConfiguration(File taskDir) {
        this.taskDir = taskDir;
    }

    public void serialize() {
        serialize(getTaskDir());
    }

    public void serialize(File dir) {
        File dest = serializedFile(dir);
        log.info("Serializing task configuration to: {}", dest);
        new Hdf5AlgorithmInterface().writeFile(dest, this, false);
    }

    public static TaskConfiguration deserialize(File taskDir) {
        File src = serializedFile(taskDir);
        TaskConfiguration taskConfiguration = new TaskConfiguration();
        log.info("Deserializing task configuration from: {}", src);
        new Hdf5AlgorithmInterface().readFile(src, taskConfiguration, false);
        return taskConfiguration;
    }

    public static boolean isSerializedTaskConfigurationPresent(File taskDir) {
        return serializedFile(taskDir).exists();
    }

    public static File serializedFile(File taskDir) {
        return new File(taskDir, PERSISTED_FILE_NAME);
    }

    public File getTaskDir() {
        return taskDir;
    }

    public void setSubtaskCount(int subtaskCount) {
        this.subtaskCount = subtaskCount;
    }

    public int getSubtaskCount() {
        return subtaskCount;
    }

    public void setInputsClass(Class<? extends PipelineInputs> inputsClass) {
        inputsClassName = inputsClass.getName();
    }

    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public Class<? extends PipelineInputs> getInputsClass() {
        try {
            return (Class<? extends PipelineInputs>) Class.forName(inputsClassName);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    public void setOutputsClass(Class<? extends PipelineOutputs> outputsClass) {
        outputsClassName = outputsClass.getName();
    }

    @SuppressWarnings("unchecked")
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public Class<? extends PipelineOutputs> getOutputsClass() {
        try {
            return (Class<? extends PipelineOutputs>) Class.forName(outputsClassName);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    public float getHeapSizeGigabytes() {
        return heapSizeGigabytes;
    }

    public void setHeapSizeGigabytes(float heapSizeGigabytes) {
        this.heapSizeGigabytes = heapSizeGigabytes;
    }

    public int getActiveCores() {
        return activeCores;
    }

    public void setActiveCores(int activeCores) {
        this.activeCores = activeCores;
    }

    public int getRequestedTimeSeconds() {
        return requestedTimeSeconds;
    }

    public void setRequestedTimeSeconds(int requestedTimeSeconds) {
        this.requestedTimeSeconds = requestedTimeSeconds;
    }

    public String getExecutableName() {
        return executableName;
    }

    public void setExecutableName(String executableName) {
        this.executableName = executableName;
    }

    // Note: it was necessary to get the class names for hashCode because you can't hash
    // a Class object itself (i.e., hash(DatastoreDirectoryPipelineInputs.class) is not
    // defined).
    @Override
    public int hashCode() {
        return Objects.hash(inputsClassName, outputsClassName, subtaskCount);
    }

    // Note: it was necessary to get the class names for equals because Class objects
    // do not define equals() (i.e., equals(DatastoreDirectoryPipelineInputs.class) is not
    // defined).
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskConfiguration other = (TaskConfiguration) obj;
        return Objects.equals(inputsClassName, other.inputsClassName)
            && Objects.equals(outputsClassName, other.outputsClassName)
            && subtaskCount == other.subtaskCount;
    }
}
