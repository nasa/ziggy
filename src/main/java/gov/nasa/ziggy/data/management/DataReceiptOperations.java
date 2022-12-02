package gov.nasa.ziggy.data.management;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;

/**
 * Performs database retrievals for information about data receipt operations.
 *
 * @author PT
 */
public class DataReceiptOperations {

    /**
     * Returns summary information for all pipeline instances that performed a data receipt
     * activity.
     *
     * @return
     */
    public List<DataReceiptInstance> dataReceiptInstances() {

        List<DataReceiptInstance> dataReceiptInstances = new ArrayList<>();

        // Get the instances that have a data receipt task in them
        List<PipelineInstance> pipelineInstances = new PipelineInstanceCrud()
            .instanceIdsForModule(DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME);

        // Construct a list entry for each instance
        for (PipelineInstance pipelineInstance : pipelineInstances) {
            DataReceiptInstance dataReceiptInstance = new DataReceiptInstance();
            dataReceiptInstance.setInstanceId(pipelineInstance.getId());
            dataReceiptInstance.setDate(pipelineInstance.getStartProcessingTime());

            dataReceiptInstance.setFailedImportCount(
                new FailedImportCrud().retrieveCountForInstance(pipelineInstance.getId()));
            dataReceiptInstance.setSuccessfulImportCount(new DatastoreProducerConsumerCrud()
                .retrieveCountForInstance(pipelineInstance.getId()));
            dataReceiptInstances.add(dataReceiptInstance);
        }
        return dataReceiptInstances;
    }

    public List<DataReceiptFile> dataReceiptFilesForInstance(long instanceId) {

        // Retrieve the successful files first
        List<DataReceiptFile> dataReceiptFiles = new DatastoreProducerConsumerCrud()
            .retrieveForInstance(instanceId)
            .stream()
            .map(DataReceiptFile::new)
            .collect(Collectors.toList());

        // Now for the failures
        dataReceiptFiles.addAll(new FailedImportCrud().retrieveForInstance(instanceId)
            .stream()
            .map(DataReceiptFile::new)
            .collect(Collectors.toList()));

        return dataReceiptFiles;
    }
}
