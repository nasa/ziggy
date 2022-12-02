package gov.nasa.ziggy.data.management;

import java.nio.file.Path;

public class DataFilePaths {

    private final Path sourcePath;
    private final Path destinationPath;
    private Path datastorePath;

    public DataFilePaths(Path sourcePath, Path destinationPath) {
        this.sourcePath = sourcePath;
        this.destinationPath = destinationPath;
    }

    public void setDatastorePathToSource() {
        datastorePath = sourcePath;
    }

    public void setDatastorePathToDestination() {
        datastorePath = destinationPath;
    }

    public Path getSourcePath() {
        return sourcePath;
    }

    public Path getDestinationPath() {
        return destinationPath;
    }

    public Path getDatastorePath() {
        return datastorePath;
    }

}
