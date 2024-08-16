/**
 * Provides classes and interfaces for all datastore operations. The heart of the package is the
 * DataFileManager class, which has the main responsibility for identifying the datastore files
 * needed as inputs for a given pipeline task, providing them to the task directory, and later
 * collecting and persisting results from each task directory. The data management package also
 * handles data receipt (the initial import of data files into the datastore). The DataFileType
 * defines the regular expressions that allow the DataFileManager to identify a given file as
 * belonging to a specified data file type.
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.data.management;
