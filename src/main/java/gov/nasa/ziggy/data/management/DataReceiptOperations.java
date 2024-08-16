package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceCrud;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Performs database retrievals for information about data receipt operations.
 *
 * @author PT
 */
public class DataReceiptOperations extends DatabaseOperations {

    private ModelCrud modelCrud = new ModelCrud();
    private PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud = new DatastoreProducerConsumerCrud();
    private FailedImportCrud failedImportCrud = new FailedImportCrud();
    private ManifestCrud manifestCrud = new ManifestCrud();

    public List<DataReceiptInstance> dataReceiptInstances() {

        return performTransaction(() -> {
            List<DataReceiptInstance> dataReceiptInstances = new ArrayList<>();

            // Get the instances that have a data receipt task in them
            List<PipelineInstance> pipelineInstances = pipelineInstanceCrud()
                .pipelineInstancesForModule(DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME);

            // Construct a list entry for each instance.
            for (PipelineInstance pipelineInstance : pipelineInstances) {
                DataReceiptInstance dataReceiptInstance = new DataReceiptInstance();
                dataReceiptInstance.setInstanceId(pipelineInstance.getId());
                dataReceiptInstance.setDate(pipelineInstance.getStartProcessingTime());

                dataReceiptInstance.setFailedImportCount(
                    failedImportCrud().retrieveCountForInstance(pipelineInstance.getId()));
                dataReceiptInstance.setSuccessfulImportCount(datastoreProducerConsumerCrud()
                    .retrieveCountForInstance(pipelineInstance.getId()));
                dataReceiptInstances.add(dataReceiptInstance);
            }
            return dataReceiptInstances;
        });
    }

    public List<DataReceiptFile> dataReceiptFilesForInstance(long instanceId) {

        return performTransaction(() -> {
            // Retrieve the successful files first.
            List<DataReceiptFile> dataReceiptFiles = datastoreProducerConsumerCrud()
                .retrieveForInstance(instanceId)
                .stream()
                .map(DataReceiptFile::new)
                .collect(Collectors.toList());

            // Now for the failures.
            dataReceiptFiles.addAll(failedImportCrud().retrieveForInstance(instanceId)
                .stream()
                .map(DataReceiptFile::new)
                .collect(Collectors.toList()));

            return dataReceiptFiles;
        });
    }

    /**
     * Updates the {@link ModelRegistry} associated with a given {@link PipelineInstance}.
     * <p>
     * A pipeline instance that includes data receipt will potentially import new models. In this
     * case, the model registry for the instance must be updated; otherwise, the pipeline modules
     * after data receipt will attempt to run using the old (pre-import) models. This method will
     * update the current pipeline instance to use the newest model registry. This is an exception
     * to the usual rule that the model registry of a pipeline instance is immutable once the
     * instance is created.
     */
    public void updateModelRegistryForPipelineInstance(long pipelineInstanceId) {
        performTransaction(() -> {
            ModelRegistry currentRegistry = modelCrud().retrieveUnlockedRegistry();
            PipelineInstance dbInstance = pipelineInstanceCrud().retrieve(pipelineInstanceId);
            dbInstance.setModelRegistry(currentRegistry);
            pipelineInstanceCrud().merge(dbInstance);
            return currentRegistry;
        });
    }

    public void persist(Manifest manifest) {
        performTransaction(() -> manifestCrud().persist(manifest));
    }

    public boolean datasetIdExists(long datasetId) {
        return performTransaction(() -> manifestCrud().datasetIdExists(datasetId));
    }

    public void createFailedImportRecords(PipelineTask pipelineTask,
        Collection<Path> failedImports) {
        performTransaction(() -> failedImportCrud().create(pipelineTask, failedImports));
    }

    ModelCrud modelCrud() {
        return modelCrud;
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }

    DatastoreProducerConsumerCrud datastoreProducerConsumerCrud() {
        return datastoreProducerConsumerCrud;
    }

    FailedImportCrud failedImportCrud() {
        return failedImportCrud;
    }

    ManifestCrud manifestCrud() {
        return manifestCrud;
    }
}
