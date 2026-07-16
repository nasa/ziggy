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
    public static final String PERSISTED_FILE_NAME = ".task-configuration.h5";
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
    private boolean memoryMonitorEnabled;
    private double memoryMonitorIntervalSeconds;

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
        setInputsClassName(inputsClass.getName());
    }

    public void setInputsClassName(String inputsClassName) {
        this.inputsClassName = inputsClassName;
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
        setOutputsClassName(outputsClass.getName());
    }

    public void setOutputsClassName(String outputsClassName) {
        this.outputsClassName = outputsClassName;
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

    public boolean isMemoryMonitorEnabled() {
        return memoryMonitorEnabled;
    }

    public void setMemoryMonitorEnabled(boolean memoryMonitorEnabled) {
        this.memoryMonitorEnabled = memoryMonitorEnabled;
    }

    public double getMemoryMonitorIntervalSeconds() {
        return memoryMonitorIntervalSeconds;
    }

    public void setMemoryMonitorIntervalSeconds(double memoryMonitorIntervalSeconds) {
        this.memoryMonitorIntervalSeconds = memoryMonitorIntervalSeconds;
    }

    // taskDir is excluded since it is not serialized.
    @Override
    public int hashCode() {
        return Objects.hash(activeCores, executableName, heapSizeGigabytes, inputsClassName,
            memoryMonitorEnabled, memoryMonitorIntervalSeconds, outputsClassName,
            requestedTimeSeconds, subtaskCount);
    }

    // taskDir is excluded since it is not serialized.
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskConfiguration other = (TaskConfiguration) obj;
        return activeCores == other.activeCores
            && Objects.equals(executableName, other.executableName)
            && Float.floatToIntBits(heapSizeGigabytes) == Float
                .floatToIntBits(other.heapSizeGigabytes)
            && Objects.equals(inputsClassName, other.inputsClassName)
            && memoryMonitorEnabled == other.memoryMonitorEnabled
            && Double.doubleToLongBits(memoryMonitorIntervalSeconds) == Double
                .doubleToLongBits(other.memoryMonitorIntervalSeconds)
            && Objects.equals(outputsClassName, other.outputsClassName)
            && requestedTimeSeconds == other.requestedTimeSeconds
            && subtaskCount == other.subtaskCount;
    }
}
