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
    private String fileType;
    private String status;

    public DataReceiptFile(DatastoreProducerConsumer record) {
        name = record.getFilename();
        taskId = record.getProducer();
        status = "Imported";
        fileType = record.getDataReceiptFileType().toString();
    }

    public DataReceiptFile(FailedImport record) {
        name = record.getFilename();
        taskId = record.getDataReceiptTaskId();
        status = "Failed";
        fileType = record.getDataReceiptFileType().toString();
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

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
