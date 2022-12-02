package gov.nasa.ziggy.ui.proxy;

import java.util.List;

import gov.nasa.ziggy.data.management.DataReceiptFile;
import gov.nasa.ziggy.data.management.DataReceiptInstance;
import gov.nasa.ziggy.data.management.DataReceiptOperations;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;

/**
 * Proxy class for {@link DataReceiptOperations}, used to perform operations of same in the context
 * of the pipeline console.
 *
 * @author PT
 */
public class DataReceiptOperationsProxy extends CrudProxy {

    public List<DataReceiptInstance> DataReceiptInstances() {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new DataReceiptOperations().dataReceiptInstances());
    }

    public List<DataReceiptFile> dataReceiptFilesForInstance(long instanceId) {
        verifyPrivileges(Privilege.PIPELINE_MONITOR);
        return ZiggyGuiConsole.crudProxyExecutor.executeSynchronousDatabaseTransaction(
            () -> new DataReceiptOperations().dataReceiptFilesForInstance(instanceId));
    }
}
