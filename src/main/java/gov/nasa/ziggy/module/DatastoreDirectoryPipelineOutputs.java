package gov.nasa.ziggy.module;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Reference implementation of the {@link PipelineOutputs} interface.
 * <p>
 * {@link DatastoreDirectoryPipelineOutputs} provides an outputs class for pipeline modules that use
 * the {@link DatastoreDirectoryUnitOfWorkGenerator} to generate units of work. It makes use of the
 * {@link DatastoreFileManager} class and the {@link DataFileType} instances that are used for
 * outputs for the current pipeline module.
 *
 * @author PT
 */
public class DatastoreDirectoryPipelineOutputs implements PipelineOutputs {

    @ProxyIgnore
    private static final Logger log = LoggerFactory
        .getLogger(DatastoreDirectoryPipelineOutputs.class);

    @ProxyIgnore
    private DatastoreFileManager datastoreFileManager;

    @ProxyIgnore
    private PipelineTask pipelineTask;

    @ProxyIgnore
    private Path taskDirectory;

    public DatastoreDirectoryPipelineOutputs() {
    }

    @Override
    public Set<Path> copyTaskFilesToDatastore() {

        log.info("Moving output files to datastore...");
        Set<Path> outputDatastoreFiles = datastoreFileManager().copyTaskDirectoryFilesToDatastore();
        log.info("Moving results files to datastore...done");
        return outputDatastoreFiles;
    }

    /**
     * Determines whether a given subtask directory contains any output files. This is done by
     * loading the collection of output data file types from the task directory and then checking
     * the files in the subtask directory for any that match any of the file name regexps for the
     * output data file types.
     */
    @Override
    public boolean subtaskProducedOutputs() {
        return subtaskProducedOutputs(PipelineInputsOutputsUtils.taskDir(),
            DirectoryProperties.workingDir());
    }

    // Broken out to simplify testing.
    boolean subtaskProducedOutputs(Path taskDir, Path workingDir) {
        Collection<DataFileType> outputDataFileTypes = PipelineInputsOutputsUtils
            .deserializedOutputFileTypesFromTaskDirectory(taskDir);
        for (DataFileType outputDataFileType : outputDataFileTypes) {
            if (!CollectionUtils
                .isEmpty(FileUtil.listFiles(workingDir, outputDataFileType.getFileNameRegexp()))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void afterAlgorithmExecution() {
        // In this case we do nothing after algorithm execution.
    }

    DatastoreFileManager datastoreFileManager() {
        if (datastoreFileManager == null) {
            datastoreFileManager = new DatastoreFileManager(getPipelineTask(), taskDirectory);
        }
        return datastoreFileManager;
    }

    @Override
    public void setPipelineTask(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    @Override
    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    @Override
    public void setTaskDirectory(Path taskDirectory) {
        this.taskDirectory = taskDirectory;
    }

    @Override
    public Path getTaskDirectory() {
        return taskDirectory;
    }
}
