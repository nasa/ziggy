package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Defines the data receipt implementation for a given pipeline. A data receipt implementation
 * consists of a set of requirements on both the overall delivery and each file in the delivery, and
 * a mapping function that maps a filename in the data receipt directory to a location in the data
 * storage system (datastore, array store, or other kind of storage). The following implementation
 * methods accomplish this:
 * <ol>
 * <li>{@link #setDataImportDirectory(Path)} sets the directory from which the current data receipt
 * task is importing files.
 * <li>{@link #isConformingDelivery()} performs checks on the overall delivery to ensure that it
 * conforms to the requirements of the {@link DataReceiptDefinition} implementation. This can be
 * activities like checking a file manifest, looking for extraneous files, etc.
 * <li>{@link #isConformingFile(Path)} performs checks on a given file in the delivery to ensure
 * that it conforms to the requirements of the {@link DataReceiptDefinition} implementation.
 * <li>{@link #successfulImports()} returns the collection of files that were successfully imported.
 * <li>{@link #failedImports()} returns the collection of files that failed to import.
 * </ol>
 * Implementations of {@link DataReceiptDefinition} must have a no-argument constructor.
 * <p>
 * The reference implementation of {@link DataReceiptDefinition} is
 * {@link DatastoreDirectoryDataReceiptDefinition}.
 *
 * @author PT
 */
public interface DataReceiptDefinition {

    /**
     * Set the path to the files for import.
     */
    void setDataImportDirectory(Path dataImportDirectory);

    /**
     * Ensures that the delivery as a whole conforms to any requirements of the data receipt system.
     * A return value of true indicates to the caller that requirements are met and that it is now
     * safe to test the files in the delivery.
     */
    boolean isConformingDelivery();

    /**
     * Ensures that each file in the delivery conforms to any requirements of the data receipt
     * system. The {@link DataReceiptPipelineModule} will loop over the files in the data receipt
     * directory, test each of them, and send a list of nonconforming files to the task log.
     */
    boolean isConformingFile(Path dataFile);

    /** Determines the data receipt {@link Path}s for all files that are to be imported. */
    List<Path> filesForImport();

    /** Performs the actual file input. */
    void importFiles();

    /**
     * {@link List} of datastore {@link Path}s for all files successfully imported.
     */
    List<Path> successfulImports();

    /**
     * {@link List} of datastore {@link Path}s for all files that failed to import.
     */
    List<Path> failedImports();

    /** Set the {@link PipelineTask} for the definition instance. */
    void setPipelineTask(PipelineTask pipelineTask);

    /**
     * Tells {@link DataReceiptPipelineModule} whether to clean the DR directories after import. The
     * default is true (i.e., do clean the DR directories). Override in order to either preserve the
     * DR directories after import, or implement a programmatic decision on whether to clean or
     * preserve the directories.
     */
    default boolean cleanDataReceiptDirectories() {
        return true;
    }
}
