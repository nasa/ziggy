package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Parent class for all data importer classes. The {@link DataImporter} implementations are used by
 * the {@link DataReceiptPipelineModule} to import mission data files into the datastore. The
 * abstract methods in this class perform assorted support functions that are needed by the
 * {@link #importFilesToDatastore(List)} method.
 *
 * @author PT
 */
public abstract class DataImporter {

    protected final PipelineTask pipelineTask;
    protected final Path datastoreRoot;
    protected final Path dataReceiptPath;
    private int invalidFilesCount;
    private int failedImportsCount;
    private int totalDataFileCount;
    private Set<Path> successfulImports;
    private Set<Path> failedImports;

    public DataImporter(PipelineTask pipelineTask, Path dataReceiptPath, Path datastoreRoot) {
        this.pipelineTask = pipelineTask;
        this.datastoreRoot = datastoreRoot;
        this.dataReceiptPath = dataReceiptPath;
    }

    /**
     * Validates the delivery as a whole.
     *
     * @return true if delivery is valid, false otherwise.
     */
    protected abstract boolean validateDelivery();

    /**
     * Validates an individual data file.
     *
     * @param dataFile {@link Path} to the file location in the data receipt directory.
     * @return true if file is valid, false otherwise.
     */
    protected abstract boolean validateDataFile(Path dataFile);

    /**
     * {@link Map} from the data receipt directory location of a file to its location in the
     * datastore. The Map key is the {@link Path} to the file prior to import, relative to the data
     * receipt path; the map value is the {@link Path} to the file after import, relative to the
     * datastore root.
     */
    protected abstract Map<Path, Path> dataFiles(List<String> namesOfValidFiles);

    /**
     * Imports files from the data receipt directory to the datastore.
     *
     * @param dataFiles {@link Map} from data receipt directory file locations to datastore file
     * locations.
     * @return {@link Set} of locations of files successfully moved to the datastore.
     */
    protected abstract Set<Path> importFiles(Map<Path, Path> dataFiles);

    /**
     * Performs the import of a given list of files.
     */
    public void importFilesToDatastore(List<String> namesOfFilesToImport) {
        // Validate the delivery
        if (!validateDelivery()) {
            throw new PipelineException("Unable to validate data delivery");
        }

        // Obtain the data file instances and validate them
        Map<Path, Path> dataFiles = dataFiles(namesOfFilesToImport);
        totalDataFileCount = dataFiles.size();
        Set<Path> invalidDataFiles = dataFiles.keySet()
            .stream()
            .filter(s -> !validateDataFile(s))
            .collect(Collectors.toSet());
        Map<Path, Path> invalidDataFilesMap = new HashMap<>();
        invalidFilesCount = invalidDataFiles.size();
        if (!invalidDataFiles.isEmpty()) {
            for (Path invalidFile : invalidDataFiles) {
                invalidDataFilesMap.put(invalidFile, dataFiles.get(invalidFile));
                dataFiles.remove(invalidFile);
            }
        }

        // Perform the import
        Set<Path> importedFiles = importFiles(dataFiles);
        Set<Path> filesNotImported = dataFiles.keySet()
            .stream()
            .filter(s -> !importedFiles.contains(s))
            .collect(Collectors.toSet());
        failedImportsCount = filesNotImported.size();
        if (!filesNotImported.isEmpty()) {
            for (Path fileNotImported : filesNotImported) {
                invalidDataFilesMap.put(fileNotImported, dataFiles.get(fileNotImported));
                dataFiles.remove(fileNotImported);
            }
        }

        // Preserve import records for use by callers.
        successfulImports = new TreeSet<>(dataFiles.values());
        failedImports = new TreeSet<>(invalidDataFilesMap.values());
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public Path getDatastoreRoot() {
        return datastoreRoot;
    }

    public Path getDataReceiptPath() {
        return dataReceiptPath;
    }

    public int getInvalidFilesCount() {
        return invalidFilesCount;
    }

    public int getFailedImportsCount() {
        return failedImportsCount;
    }

    public int getTotalDataFileCount() {
        return totalDataFileCount;
    }

    public Set<Path> getSuccessfulImports() {
        return successfulImports;
    }

    public void setSuccessfulImports(Set<Path> successfulImports) {
        this.successfulImports = successfulImports;
    }

    public Set<Path> getFailedImports() {
        return failedImports;
    }

    public void setFailedImports(Set<Path> failedImports) {
        this.failedImports = failedImports;
    }
}
