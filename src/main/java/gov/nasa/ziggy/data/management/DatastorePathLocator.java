package gov.nasa.ziggy.data.management;

import java.nio.file.Path;

/**
 * Provides logic that determines the path in the datastore to a given file, given the DataStoreFile
 * for that file. Each pipeline that uses Ziggy must supply at least one implementation of
 * DatastorePathLocator that can be used by pipeline modules and by the DataFileManager.
 *
 * @author PT
 */
public interface DatastorePathLocator {

    /**
     * Determines the Path of a datastore file, given an instance of a DataFileInfo subclass.
     *
     * @param dataFileInfo non-null, valid instance of DataFileInfo subclass.
     * @return Path for corresponding file.
     */
    Path datastorePath(DataFileInfo dataFileInfo);
}
