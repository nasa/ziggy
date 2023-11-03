package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.module.PipelineInputsOutputsUtils.moduleName;
import static gov.nasa.ziggy.module.PipelineInputsOutputsUtils.taskDir;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.management.DataFileInfo;
import gov.nasa.ziggy.data.management.DataFileManager;
import gov.nasa.ziggy.data.management.DatastorePathLocator;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Superclass for all pipeline outputs classes. The pipeline outputs class for a given pipeline
 * module contains all the results of processing a given sub-task for a given module. The class also
 * performs the following functions:
 * <ol>
 * <li>Identifies all the subclasses of DataFileInfo that are produced by the pipeline
 * (see @link{reqiredDataFileInfoClasses()}).
 * <li>Identifies all the files in the sub-task directory that are pipeline outputs based on the
 * name convention for pipeline outputs files (see @link{outputFiles()}).</li>
 * <li>Deserializes the contents of a specific sub-task outputs HDF5 file
 * (see @link{readSubTaskOutputs(File file)}).</li>
 * <li>Uses the contents of a sub-task's outputs to construct a map between DatastoreId subclass
 * instances and PipelineResults subclass instances
 * (see @link{createPipelineResultsByDatastoreIdMap()}).</li>
 * <li>Detects the task ID by parsing the task directory name so that this can be added to all
 * PipelineResults subclass instances (see @link{originator()}).</li>
 * <li>Serializes the PipelineResults subclass instances as HDF5 in the task directory
 * (see @link{saveResultsToTaskDir()}).</li>
 * </ol>
 * Once the above steps have been performed, the pipeline module can copy the results files from the
 * task directory to the datastore.
 * <p>
 * The abstract method @link{populateTaskResults()} manages the process of reading outputs,
 * redistributing their contents to PipelineResults subclass instances, and writing same to the task
 * directory.
 * <p>
 * The method @link{copyTaskDirectoryResultsToDatastore(DatastorePathLocator datastorePathLocator,
 * PipelineTask pipelineTask, Path taskDirectory, ProcessingFailureSummary failureSummary) provides
 * a standard default method that can be used to copy results files from the task directory to the
 * datastore, and to delete unneeded datastore file copies from the task directory.
 *
 * @author PT
 */
public abstract class PipelineOutputs implements Persistable {

    private static final Logger log = LoggerFactory.getLogger(PipelineOutputs.class);

    @ProxyIgnore
    private Long originator;

    @ProxyIgnore
    Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();

    /**
     * Returns a non-{@code null} set of DataFileInfo subclasses that are produced by the pipeline
     * module. Concrete subclasses of this class should override this with a method that returns the
     * needed DatastoreId classes.
     */
    public Set<Class<? extends DataFileInfo>> requiredDataFileInfoClasses() {
        return Collections.emptySet();
    }

    /**
     * Converts the contents of the outputs file in the sub-task directory into one or more results
     * files in the task directory.
     */
    public abstract void populateTaskResults();

    /**
     * Determines whether the subtask that was processed in the current working directory produced
     * results, and if so creates a zero-length file in the directory that indicates that results
     * were produced. The zero-length file is created by
     * {@link AlgorithmStateFiles#setResultsFlag()}. The determination regarding the presence or
     * absence of results is performed by the abstract boolean method
     * {@link #subtaskProducedResults()}. This allows the persisting code to determine, for each
     * subtask, whether or not that subtask produced results. Note that a subtask can run to
     * completion but not produce results.
     */
    public void setResultsState() {
        if (subtaskProducedResults()) {
            new AlgorithmStateFiles(DirectoryProperties.workingDir().toFile()).setResultsFlag();
        }
    }

    /**
     * Determines whether the subtask that ran in the current working directory produced results. A
     * subtask that runs to completion may nonetheless produce no results, in which case the subtask
     * will not be counted as failed and will not be re-run in the event that the task is
     * resubmitted. The determination as to whether a subtask produced results is potentially
     * implementation-specific: for example, an algorithm that produces multiple different results
     * files for each subtask may want to identify a subset of those results files to use in the
     * determination of whether results were produced, which would allow some results files to be
     * necessary to the determination but others optional.
     *
     * @return true if all required results files were produced for a given subtask, false
     * otherwise.
     */
    protected abstract boolean subtaskProducedResults();

    /**
     * Returns an array of files with names that match the pipeline convention for output files for
     * a the given CSCI.
     *
     * @return
     */
    public File[] outputFiles() {
        File workingDir = DirectoryProperties.workingDir().toFile();
        File[] detectedFiles = workingDir.listFiles((FileFilter) pathname -> {
            String filename = pathname.getName();
            Pattern p = ModuleInterfaceUtils.outputsFileNamePattern(moduleName());
            Matcher m = p.matcher(filename);
            return m.matches();
        });
        log.info("Number of output files detected: " + detectedFiles.length);
        return detectedFiles;
    }

    /**
     * Populates the outputs instance from an HDF5 file
     *
     * @param file
     */
    public void readSubTaskOutputs(File file) {
        log.info("Reading file " + file.getName() + " into memory");
        hdf5ModuleInterface.readFile(file, this, true);
    }

    /**
     * Generates a map from DataFileInfo to pipeline results instances that are populated from this
     * PipelineOutputs instance.
     *
     * @return
     */
    public abstract Map<DataFileInfo, PipelineResults> pipelineResults();

    /**
     * Returns the originator for this set of outputs.
     *
     * @return
     */
    public long originator() {
        if (originator == null) {
            String taskDirName = taskDir().getFileName().toString();
            String regex = "\\d+-(\\d+)-\\w+";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(taskDirName);
            matcher.matches();
            originator = Long.valueOf(matcher.group(1));
        }
        return originator;
    }

    /**
     * Saves the results to HDF5 files in the task directory. The results that are saved are all the
     * ones produced by the PipelineResults() method. The results instances are populated with the
     * originator prior to saving.
     */
    public void saveResultsToTaskDir() {
        saveResultsToTaskDir(pipelineResults());
    }

    /**
     * Saves results to HDF5 files in the task directory. The results that are saved must be in a
     * caller-provided Map between DatastoreId instances and PipelineResults instances. The results
     * instances are populated with the originator prior to saving.
     *
     * @param resultsMap
     */
    public void saveResultsToTaskDir(Map<DataFileInfo, PipelineResults> resultsMap) {

        for (DataFileInfo dataFileInfo : resultsMap.keySet()) {
            PipelineResults result = resultsMap.get(dataFileInfo);
            result.setOriginator(originator());
            log.info("Writing file " + dataFileInfo.getName().toString() + " to task directory");
            hdf5ModuleInterface.writeFile(taskDir().resolve(dataFileInfo.getName()).toFile(),
                result, true);
        }
    }

    /**
     * Performs the final persistence of results from the pipeline and clean-up of datastore files
     * in the task directory. NB: if a pipeline requires a more complex final persistence and
     * clean-up than is provided here, the outputs class for that pipeline should override this
     * method.
     *
     * @param datastorePathLocator Instance of a DatastorePathLocator subclass for this task.
     * @param pipelineTask Pipeline task for this task.
     * @param taskDirectory Task directory for this task.
     */
    public void copyTaskDirectoryResultsToDatastore(DatastorePathLocator datastorePathLocator,
        PipelineTask pipelineTask, Path taskDirectory) {

        DataFileManager fileManager = new DataFileManager(datastorePathLocator, pipelineTask,
            taskDirectory);

        // Move the results files from the task directory to the datastore.
        Set<Class<? extends DataFileInfo>> dataFileInfoClasses = requiredDataFileInfoClasses();
        Set<? extends DataFileInfo> outputsSet = fileManager.datastoreFiles(taskDirectory,
            dataFileInfoClasses);
        fileManager.moveToDatastore(outputsSet);
    }

    /**
     * Updates the set of consumers for files that are used as inputs by the pipeline. Only files
     * that were used in at least one subtask that completed successfully will be recorded in the
     * database.
     */
    public void updateInputFileConsumers(PipelineInputs pipelineInputs, PipelineTask pipelineTask,
        Path taskDirectory) {

        DatastorePathLocator datastorePathLocator = pipelineInputs
            .datastorePathLocator(pipelineTask);
        DataFileManager fileManager = new DataFileManager(datastorePathLocator, pipelineTask,
            taskDirectory);

        Set<String> filenames = fileManager
            .filesInCompletedSubtasksWithResults(pipelineInputs.requiredDataFileInfoClasses());
        DatastoreProducerConsumerCrud producerConsumerCrud = new DatastoreProducerConsumerCrud();
        producerConsumerCrud.addConsumer(pipelineTask, filenames);

        filenames = fileManager
            .filesInCompletedSubtasksWithoutResults(pipelineInputs.requiredDataFileInfoClasses());
        producerConsumerCrud.addNonProducingConsumer(pipelineTask, filenames);
    }

    protected Hdf5ModuleInterface hdf5ModuleInterface() {
        return hdf5ModuleInterface;
    }
}
