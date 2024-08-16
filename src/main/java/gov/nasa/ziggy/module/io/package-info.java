/**
 * Provides classes and interfaces for the interchange of information between Ziggy and algorithm
 * processes. Any Java class that is to be exchanged in the form of HDF5 files must implement the
 * {@link gov.nasa.ziggy.module.io.Persistable} interface. The
 * {@link gov.nasa.ziggy.module.io.ProxyIgnore} annotation allows a field in a Java class to be
 * skipped when reading and writing HDF5.
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.module.io;
