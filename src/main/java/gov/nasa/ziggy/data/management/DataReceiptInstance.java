package gov.nasa.ziggy.data.management;

import java.util.Date;

/**
 * Contains summary information for data receipt tasks within a single pipeline instance.
 *
 * @author PT
 */
public class DataReceiptInstance {

    private long instanceId;
    private Date date;
    private int successfulImportCount;
    private int failedImportCount;

    public long getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(long instanceId) {
        this.instanceId = instanceId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getSuccessfulImportCount() {
        return successfulImportCount;
    }

    public void setSuccessfulImportCount(int successfulImportCount) {
        this.successfulImportCount = successfulImportCount;
    }

    public int getFailedImportCount() {
        return failedImportCount;
    }

    public void setFailedImportCount(int failedImportCount) {
        this.failedImportCount = failedImportCount;
    }
}
