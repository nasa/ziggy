package gov.nasa.ziggy.module;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.SpotBugsUtils;

/**
 * Serializes and deserializes the subtask count, inputs class, and outputs class for a given
 * {@link PipelineTask}.
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskConfiguration implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(TaskConfiguration.class);
    private static final long serialVersionUID = 20240112L;
    private static final String PERSISTED_FILE_NAME = ".task-configuration.ser";
    public static final String LOCK_FILE_NAME = ".lock";

    private transient File taskDir = null;

    private Class<? extends PipelineInputs> inputsClass;
    private Class<? extends PipelineOutputs> outputsClass;
    private int subtaskCount;

    public TaskConfiguration() {
    }

    public TaskConfiguration(File taskDir) {
        this.taskDir = taskDir;
    }

    public void serialize() {
        serialize(getTaskDir());
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void serialize(File dir) {
        File dest = serializedFile(dir);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dest))) {
            log.info("Serializing task configuration to: {}", dest);
            oos.writeObject(this);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to serialize task configuration to " + dir.toString(), e);
        }
    }

    @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION",
        justification = SpotBugsUtils.DESERIALIZATION_JUSTIFICATION)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    public static TaskConfiguration deserialize(File taskDir) {
        File src = serializedFile(taskDir);
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(src))) {
            log.info("Deserializing task configuration from: {}", src);

            TaskConfiguration s = (TaskConfiguration) ois.readObject();
            s.taskDir = taskDir;

            return s;
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to deserialize configuration manager from " + taskDir.toString(), e);
        } catch (ClassNotFoundException e) {
            // This can never occur. By construction, the object deserialized here was
            // serialized at some prior point by this same class, which means that it is
            // guaranteed to be a TaskConfigurationManager instance.
            throw new AssertionError(e);
        }
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
        this.inputsClass = inputsClass;
    }

    public Class<? extends PipelineInputs> getInputsClass() {
        return inputsClass;
    }

    public void setOutputsClass(Class<? extends PipelineOutputs> outputsClass) {
        this.outputsClass = outputsClass;
    }

    public Class<? extends PipelineOutputs> getOutputsClass() {
        return outputsClass;
    }

    // Note: it was necessary to get the class names for hashCode because you can't hash
    // a Class object itself (i.e., hash(DatastoreDirectoryPipelineInputs.class) is not
    // defined).
    @Override
    public int hashCode() {
        return Objects.hash(inputsClass.getName(), outputsClass.getName(), subtaskCount);
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
        return Objects.equals(inputsClass.getName(), other.inputsClass.getName())
            && Objects.equals(outputsClass.getName(), other.outputsClass.getName())
            && subtaskCount == other.subtaskCount;
    }
}
