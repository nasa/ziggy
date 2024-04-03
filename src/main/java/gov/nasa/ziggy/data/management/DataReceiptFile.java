package gov.nasa.ziggy.data.management;

/**
 * Contains summary information on a single file that was either successfully imported or which
 * failed to import correctly.
 *
 * @author PT
 */
public class DataReceiptFile {

    private long taskId;
    private String name;
    private String status;

    public DataReceiptFile(DatastoreProducerConsumer record) {
        name = record.getFilename();
        taskId = record.getProducer();
        status = "Imported";
    }

    public DataReceiptFile(FailedImport record) {
        name = record.getFilename();
        taskId = record.getDataReceiptTaskId();
        status = "Failed";
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
