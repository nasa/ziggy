/**
 * Provides classes and interfaces for HDF5 communication. Ziggy uses HDF5 as its interchange format
 * for specifying inputs and outputs to algorithm processes. Arrays of primitives, Strings, and
 * enums are managed by instances of {@link gov.nasa.ziggy.module.hdf5.PrimitiveHdf5Array}. Arrays
 * of more complex Java types are managed by instances of
 * {@link gov.nasa.ziggy.module.hdf5.PersistableHdf5Array}. Pipeline module parameter sets are
 * managed by {@link gov.nasa.ziggy.module.hdf5.ModuleParametersHdf5Array}. An instance of
 * PersistableHdf5Array can contain fields that are instances of PrimitiveHdf5Array,
 * ModuleParametersHdf5Array, or PersistableHdf5Array instances. In this way, the full data
 * hierarchy represented by a Java object can be translated to HDF5, and vice-versa.
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.module.hdf5;
