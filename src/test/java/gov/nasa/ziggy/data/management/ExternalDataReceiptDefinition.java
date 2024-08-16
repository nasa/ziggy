package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

public class ExternalDataReceiptDefinition implements DataReceiptDefinition {

    private boolean dataImportDirectorySet;
    private boolean conformingDeliveryChecked;
    private boolean conformingFileChecked;
    private boolean filesForImportDetermined;
    private boolean filesImported;
    private boolean successfulImportsDetermined;
    private boolean failedImportsDetermined;
    private boolean pipelineTaskSet;
    private boolean dataReceiptDirectoryCleaningChecked;

    public ExternalDataReceiptDefinition() {
    }

    @Override
    public void setDataImportDirectory(Path dataImportDirectory) {
        dataImportDirectorySet = true;
    }

    @Override
    public boolean isConformingDelivery() {
        conformingDeliveryChecked = true;
        return true;
    }

    @Override
    public boolean isConformingFile(Path dataFile) {
        conformingFileChecked = true;
        return true;
    }

    @Override
    public List<Path> filesForImport() {
        filesForImportDetermined = true;
        return List.of(Paths.get("pathtest"));
    }

    @Override
    public void importFiles() {
        filesImported = true;
    }

    @Override
    public List<Path> successfulImports() {
        successfulImportsDetermined = true;
        return new ArrayList<>();
    }

    @Override
    public List<Path> failedImports() {
        failedImportsDetermined = true;
        return new ArrayList<>();
    }

    @Override
    public void setPipelineTask(PipelineTask pipelineTask) {
        pipelineTaskSet = true;
    }

    @Override
    public boolean cleanDataReceiptDirectories() {
        dataReceiptDirectoryCleaningChecked = true;
        return false;
    }

    public boolean isDataImportDirectorySet() {
        return dataImportDirectorySet;
    }

    public boolean isConformingDeliveryChecked() {
        return conformingDeliveryChecked;
    }

    public boolean isConformingFileChecked() {
        return conformingFileChecked;
    }

    public boolean isFilesForImportDetermined() {
        return filesForImportDetermined;
    }

    public boolean isFilesImported() {
        return filesImported;
    }

    public boolean isSuccessfulImportsDetermined() {
        return successfulImportsDetermined;
    }

    public boolean isFailedImportsDetermined() {
        return failedImportsDetermined;
    }

    public boolean isPipelineTaskSet() {
        return pipelineTaskSet;
    }

    public boolean isDataReceiptDirectoryCleaningChecked() {
        return dataReceiptDirectoryCleaningChecked;
    }
}
