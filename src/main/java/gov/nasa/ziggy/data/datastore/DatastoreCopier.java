package gov.nasa.ziggy.data.datastore;

import java.nio.file.Path;

/**
 * Defines the copy operation used by {@link DatastoreFileManager}.
 *
 * @author PT
 */
public interface DatastoreCopier {

    void copy(Path src, Path dest);
}
