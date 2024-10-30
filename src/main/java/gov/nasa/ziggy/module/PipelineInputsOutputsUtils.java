package gov.nasa.ziggy.module;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Provides utility functions for PipelineInputs and PipelineOutputs classes.
 *
 * @author PT
 */
public abstract class PipelineInputsOutputsUtils implements Persistable {

    private static final String SERIALIZED_OUTPUTS_TYPE_FILE = ".output-types.ser";

    /**
     * Returns the task directory. Assumes that the working directory is the subtask directory.
     */
    public static Path taskDir() {
        return DirectoryProperties.workingDir().getParent();
    }

    /**
     * Returns the module executable name. Assumes that the working directory is the subtask
     * directory.
     */
    public static String moduleName() {
        return moduleName(taskDir());
    }

    public static String moduleName(Path taskDir) {
        return new PipelineTask.TaskBaseNameMatcher(taskDir.getFileName().toString()).moduleName();
    }

    public static long taskId(Path taskDir) {
        return new PipelineTask.TaskBaseNameMatcher(taskDir.getFileName().toString()).taskId();
    }

    /**
     * Applies the log stream identifier to the current thread, so that log messages will contain
     * information on which subtask generated them.
     */
    public static void putLogStreamIdentifier() {
        String subtaskName = DirectoryProperties.workingDir().getFileName().toString();
        SubtaskUtils.putLogStreamIdentifier(subtaskName);
    }

    /** Writes an instance of {@link PipelineInputs} to a directory. */
    public static void writePipelineInputsToDirectory(PipelineInputs inputs, String moduleName,
        Path directory) {
        String filename = ModuleInterfaceUtils.inputsFileName(moduleName);
        File inputInTaskDir = new File(directory.toFile(), filename);
        new Hdf5ModuleInterface().writeFile(inputInTaskDir, inputs, true);
    }

    /** Reads an instance of {@link PipelineInputs} from a directory. */
    public static void readPipelineInputsFromDirectory(PipelineInputs inputs, String moduleName,
        Path directory) {
        String filename = ModuleInterfaceUtils.inputsFileName(moduleName);
        File inputInTaskDir = new File(directory.toFile(), filename);
        new Hdf5ModuleInterface().readFile(inputInTaskDir, inputs, true);
    }

    public static void writePipelineOutputsToDirectory(PipelineOutputs outputs, String moduleName,
        Path directory) {
        String filename = ModuleInterfaceUtils.outputsFileName(moduleName);
        File outputInTaskDir = new File(directory.toFile(), filename);
        new Hdf5ModuleInterface().writeFile(outputInTaskDir, outputs, true);
    }

    public static void readPipelineOutputsFromDirectory(PipelineOutputs outputs, String moduleName,
        Path directory) {
        String filename = ModuleInterfaceUtils.outputsFileName(moduleName);
        File outputInTaskDir = new File(directory.toFile(), filename);
        new Hdf5ModuleInterface().readFile(outputInTaskDir, outputs, true);
    }

    /**
     * Returns an instance of {@link PipelineInputs} with its {@link PipelineTask} and {@link Path}
     * to the task directory initialized.
     */
    public static PipelineInputs newPipelineInputs(ClassWrapper<PipelineInputs> inputsClass,
        PipelineTask pipelineTask, Path taskDirectory) {
        PipelineInputs pipelineInputs = inputsClass.newInstance();
        pipelineInputs.setPipelineTask(pipelineTask);
        pipelineInputs.setTaskDirectory(taskDirectory);
        return pipelineInputs;
    }

    /**
     * Returns an instance of {@link PipelineInputs} with its {@link PipelineTask} and {@link Path}
     * to the task directory initialized.
     */
    public static PipelineOutputs newPipelineOutputs(ClassWrapper<PipelineOutputs> outputsClass,
        PipelineTask pipelineTask, Path taskDirectory) {
        PipelineOutputs pipelineOutputs = outputsClass.newInstance();
        pipelineOutputs.setPipelineTask(pipelineTask);
        pipelineOutputs.setTaskDirectory(taskDirectory);
        return pipelineOutputs;
    }

    /** Serializes the output data file types for a task to the task directory. */
    public static void serializeOutputFileTypesToTaskDirectory(
        Collection<DataFileType> outputDataFileTypes, Path taskDirectory) {
        Path serializationPath = taskDirectory.resolve(SERIALIZED_OUTPUTS_TYPE_FILE);
        try (ObjectOutputStream oos = new ObjectOutputStream(
            new FileOutputStream(serializationPath.toFile()))) {
            oos.writeObject(outputDataFileTypes);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to persist output data file types to " + taskDirectory.toString(), e);
        }
    }

    /** Deserializes the output data file types for a task from the task directory. */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.CAN_NEVER_OCCUR)
    @SuppressWarnings("unchecked")
    public static Collection<DataFileType> deserializedOutputFileTypesFromTaskDirectory(
        Path taskDirectory) {
        Path deserializationPath = taskDirectory.resolve(SERIALIZED_OUTPUTS_TYPE_FILE);
        try (ObjectInputStream ois = new ObjectInputStream(
            new FileInputStream(deserializationPath.toFile()))) {
            return (Collection<DataFileType>) ois.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to deserialize output file types from " + taskDirectory.toString(), e);
        } catch (ClassNotFoundException e) {
            // This should never occur because the DataFileType class is guaranteed to on the
            // classpath and Collection is part of Java.
            throw new AssertionError(e);
        }
    }
}
