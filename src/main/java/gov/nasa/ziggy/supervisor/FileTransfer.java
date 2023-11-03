package gov.nasa.ziggy.supervisor;

/**
 * Models a file to be transfered to a worker process using the Messaging Service. Typical use case
 * is for updating code, config files, module shared libraries, etc. on workers from a central
 * console/GUI
 *
 * @author Todd Klaus
 */
public class FileTransfer {
    /**
     * absolute path on sender machine
     */
    private String sourcePath;

    /**
     * relative path - will be written relative to worker's root directory
     */
    private String destPath;

    public FileTransfer() {
    }

    /**
     * @param destPath
     * @param sourcePath
     */
    public FileTransfer(String sourcePath, String destPath) {
        this.sourcePath = sourcePath;
        this.destPath = destPath;
    }

    /**
     * @return Returns the destPath.
     */
    public String getDestPath() {
        return destPath;
    }

    /**
     * @param destPath The destPath to set.
     */
    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }

    /**
     * @return Returns the sourcePath.
     */
    public String getSourcePath() {
        return sourcePath;
    }

    /**
     * @param sourcePath The sourcePath to set.
     */
    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }
}
