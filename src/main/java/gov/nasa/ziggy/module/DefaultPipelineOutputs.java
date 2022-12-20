package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.module.PipelineInputsOutputsUtils.moduleName;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.management.DataFileInfo;
import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.data.management.DatastorePathLocator;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Default pipeline outputs class for pipeline modules that use DataFileType instances to define
 * their data file needs.
 * <p>
 * The DefaultPipelineOutputs class can only be used in cases in which the pipeline module produces
 * files in the subtask directory that can be copied to the datastore without any reorganization of
 * their contents. In cases where some reorganization of the module outputs is required to obtain
 * results that can be saved, users are directed to write their own extensions to the
 * PipelineOutputs abstract class.
 *
 * @author PT
 */
public class DefaultPipelineOutputs extends PipelineOutputs {

    @ProxyIgnore
    private static final Logger log = LoggerFactory.getLogger(DefaultPipelineOutputs.class);

    @ProxyIgnore
    private DataFileManager dataFileManager;

    public DefaultPipelineOutputs() {

    }

    /**
     * Constructor for test purposes, which allows a modified DataFileManager to be inserted.
     */
    DefaultPipelineOutputs(DataFileManager dataFileManager) {
        this.dataFileManager = dataFileManager;
    }

    /**
     * Copies results files from the subtask directory to the task directory. The results files are
     * identified by their filenames, which match the regular expressions for outputs data file
     * types. The outputs data file types are stored in the DefaultPipelineInputs HDF5 file, which
     * must be loaded to obtain the desired information.
     */
    @Override
    public void populateTaskResults() {

        PipelineInputsOutputsUtils.putLogStreamIdentifier();
        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        log.info("Copying outputs files to task directory...");
        dataFileManager(null, taskDir, null)
            .copyDataFilesByTypeFromWorkingDirToTaskDir(outputDataFileTypes());
        log.info("Copying outputs files to task directory...complete");
    }

    private Set<DataFileType> outputDataFileTypes() {
        Path taskDir = PipelineInputsOutputsUtils.taskDir();
        // Deserialize the DefaultPipelineInputs instance
        DefaultPipelineInputs inputs = new DefaultPipelineInputs();
        String filename = ModuleInterfaceUtils.inputsFileName(moduleName());
        hdf5ModuleInterface.readFile(new File(taskDir.toFile(), filename), inputs, true);
        Set<DataFileType> dataFileTypes = new HashSet<>(inputs.getOutputDataFileTypes());
        return dataFileTypes;
    }

    /**
     * The pipelineResults() method is not used by the DefaultPipelineOutputs workflow.
     */
    @Override
    public Map<DataFileInfo, PipelineResults> pipelineResults() {
        return null;
    }

    /**
     * Moves results files from the task directory to the datastore.
     */
    @Override
    public void copyTaskDirectoryResultsToDatastore(DatastorePathLocator locator,
        PipelineTask pipelineTask, Path taskDir) {

        log.info("Moving results files to datastore...");
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        DataFileManager dataFileManager = dataFileManager(datastoreRoot, taskDir, pipelineTask);
        Set<DataFileType> outputDataFileTypes = pipelineTask.getPipelineDefinitionNode()
            .getOutputDataFileTypes();
        dataFileManager.moveDataFilesByTypeToDatastore(outputDataFileTypes);
        log.info("Moving results files to datastore...complete");
    }

    /**
     * Updates the set of consumers for files that are used as inputs by the pipeline. Only files
     * that were used in at least one subtask that completed successfully will be recorded in the
     * database.
     */
    @Override
    public void updateInputFileConsumers(PipelineInputs pipelineInputs, PipelineTask pipelineTask,
        Path taskDirectory) {
        log.info("Updating input file consumers...");
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        DataFileManager dataFileManager = dataFileManager(datastoreRoot, taskDirectory,
            pipelineTask);
        Set<String> consumedInputFiles = dataFileManager
            .datastoreFilesInCompletedSubtasksWithResults(
                pipelineTask.getPipelineDefinitionNode().getInputDataFileTypes());
        DatastoreProducerConsumerCrud producerConsumerCrud = new DatastoreProducerConsumerCrud();
        producerConsumerCrud.addConsumer(pipelineTask, consumedInputFiles);

        Set<String> consumedInputFilesWithoutResults = dataFileManager
            .datastoreFilesInCompletedSubtasksWithoutResults(
                pipelineTask.getPipelineDefinitionNode().getInputDataFileTypes());
        producerConsumerCrud.addNonProducingConsumer(pipelineTask,
            consumedInputFilesWithoutResults);

        log.info("Updating input file consumers...complete");
    }

    private DataFileManager dataFileManager(Path datastoreRoot, Path taskDir,
        PipelineTask pipelineTask) {
        if (dataFileManager == null) {
            dataFileManager = new DataFileManager(datastoreRoot, taskDir, pipelineTask);
        }
        return dataFileManager;
    }

    @Override
    protected boolean subtaskProducedResults() {
        return dataFileManager(null, PipelineInputsOutputsUtils.taskDir(), null)
            .workingDirHasFilesOfTypes(outputDataFileTypes());
    }

}
