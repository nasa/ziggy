package gov.nasa.ziggy.ui.util.proxy;

import java.util.List;

import gov.nasa.ziggy.data.management.DataReceiptFile;
import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.data.management.DataReceiptOperations;

/**
 * Proxy class for {@link DataReceiptOperations}, used to perform operations of same in the context
 * of the pipeline console.
 *
 * @author PT
 */
public class DataReceiptOperationsProxy {

    public List<DataReceiptInstance> dataReceiptInstances() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new DataReceiptOperations().dataReceiptInstances());
    }

    public List<DataReceiptFile> dataReceiptFilesForInstance(long instanceId) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new DataReceiptOperations().dataReceiptFilesForInstance(instanceId));
    }
}
