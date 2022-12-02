package gov.nasa.ziggy.data.management;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Default implementation of the {@link DataImporter} interface. This class can be used for data
 * receipt subject to the following restrictions:
 * <ol>
 * <li>The unit of work for the data receipt task must be {@link DataReceiptUnitOfWorkGenerator}.
 * <li>The files for receipt must have names that match the task directory regular expression for
 * one of the {@link DataFileType} instances passed to the {@link DataImporter} instance.
 * <li>The destination in the datastore for each file must be specified by the datastore file name
 * formulation of the relevant {@link DataFileType}.
 * </ol>
 *
 * @author PT
 */
public class DefaultDataImporter extends DataImporter {

    private final Path dataImportPath;

    public DefaultDataImporter(PipelineTask pipelineTask, Path dataReceiptPath,
        Path datastoreRoot) {
        super(pipelineTask, dataReceiptPath, datastoreRoot);

        // Obtain the UOW
        UnitOfWork uow = pipelineTask.getUowTask().getInstance();
        dataImportPath = dataReceiptPath.resolve(DirectoryUnitOfWorkGenerator.directory(uow));
    }

    private Logger log = LoggerFactory.getLogger(DataImporter.class);

    @Override
    public boolean validateDelivery() {
        return true;
    }

    @Override
    public boolean validateDataFile(Path dataFile) {
        return true;
    }

    @Override
    public Map<Path, Path> dataFiles(List<String> namesOfValidFiles) {

        log.info("Importing data files from directory: " + dataImportPath.toString());

        // Get the set of input data file types from the pipeline task
        Set<DataFileType> dataTypes = pipelineTask.getPipelineDefinitionNode()
            .getInputDataFileTypes();

        // Find the files that match one of the data file types, and generate the
        // corresponding datastore name
        Map<Path, Path> dataFiles = new HashMap<>();
        for (DataFileType dataFileType : dataTypes) {
            Pattern pattern = dataFileType.fileNamePatternForTaskDir();
            List<String> matchingFilenames = namesOfValidFiles.stream()
                .filter(s -> pattern.matcher(s).matches())
                .collect(Collectors.toList());
            log.info("Found " + matchingFilenames.size() + " files that match data type \""
                + dataFileType.getName() + "\"");
            for (String filename : matchingFilenames) {
                dataFiles.put(Paths.get(filename),
                    Paths.get(dataFileType.datastoreFileNameFromTaskDirFileName(filename)));
            }
        }
        return dataFiles;
    }

    @Override
    public Set<Path> importFiles(Map<Path, Path> dataFiles) {
        Set<Path> importedFiles = new HashSet<>();
        Set<Path> datastoreDirectories = new HashSet<>();
        for (Path destPath : dataFiles.values()) {
            datastoreDirectories.add(destPath.getParent());
        }
        for (Path destDir : datastoreDirectories) {
            datastoreRoot.resolve(destDir).toFile().mkdirs();
        }
        for (Path sourceFile : dataFiles.keySet()) {
            Path fullSourcePath = dataImportPath.resolve(sourceFile);
            Path fullDestPath = datastoreRoot.resolve(dataFiles.get(sourceFile));
            try {
                moveOrSymlink(fullSourcePath, fullDestPath);
                importedFiles.add(sourceFile);
            } catch (IOException e) {
                log.error("Unable to import data file " + sourceFile.toString(), e);
            }
        }
        return importedFiles;
    }

    // Delegate in order to support testing of the case in which
    // an IOException occurs.
    void moveOrSymlink(Path fullSourcePath, Path fullDestPath) throws IOException {
        DataFileManager.moveOrSymlink(fullSourcePath, fullDestPath);
    }

    public Path getDataImportPath() {
        return dataImportPath;
    }

}
