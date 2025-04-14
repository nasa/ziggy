package gov.nasa.ziggy.pipeline.step.io;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreCopier;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.util.io.ProxyIgnore;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Reference implementation of the {@link PipelineOutputs} interface.
 * <p>
 * {@link DatastoreDirectoryPipelineOutputs} provides an outputs class for pipeline steps that use
 * the {@link DatastoreDirectoryUnitOfWorkGenerator} to generate units of work. It makes use of the
 * {@link DatastoreFileManager} class and the {@link DataFileType} instances that are used for
 * outputs for the current pipeline step.
 *
 * @author PT
 */
public class DatastoreDirectoryPipelineOutputs extends PipelineOutputs {

    @ProxyIgnore
    private static final Logger log = LoggerFactory
        .getLogger(DatastoreDirectoryPipelineOutputs.class);

    @ProxyIgnore
    private DatastoreFileManager datastoreFileManager;

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
            if (!CollectionUtils.isEmpty(ZiggyFileUtils.listFiles(workingDir,
                DatastoreWalker.fileNameRegexpBaseName(outputDataFileType)))) {
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
            datastoreFileManager = new DatastoreFileManager(getPipelineTask(), getTaskDirectory());
            if (taskDirToDatastoreCopier() != null) {
                datastoreFileManager.setTaskDirToDatastoreCopier(taskDirToDatastoreCopier());
            }
        }
        return datastoreFileManager;
    }

    /**
     * Determines the {@link DatastoreCopier} that will be used by the {@link DatastoreFileManager}
     * to copy files from the task directory to the datastore. Subclasses can override this method
     * if they want the file copier to do something different from its nominal behavior (example:
     * use symbolic links instead of hard links, etc.).
     */
    public DatastoreCopier taskDirToDatastoreCopier() {
        return null;
    }
}
