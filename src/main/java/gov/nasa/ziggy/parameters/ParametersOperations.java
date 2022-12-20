package gov.nasa.ziggy.parameters;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import jakarta.xml.bind.JAXBException;

/**
 * Utility functions for managing the parameter library.
 *
 * @author Forrest Girouard
 * @author Todd Klaus
 * @author PT
 */
public class ParametersOperations {
    private static final Logger log = LoggerFactory.getLogger(ParametersOperations.class);

    private ValidatingXmlManager<ParameterLibrary> xmlManager;

    public ParametersOperations() {
        try {
            xmlManager = new ValidatingXmlManager<>(ParameterLibrary.class);
        } catch (InstantiationException | IllegalAccessException | SAXException | JAXBException
            | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
            | SecurityException e) {
            throw new PipelineException(
                "Unable to construct ValidatingXmlManager for class ParameterLibrary", e);
        }

    }

    /**
     * Export the current parameter library to the specified file.
     *
     * @param destinationPath
     * @throws IOException
     */
    public List<ParameterSetDescriptor> exportParameterLibrary(String destinationPath,
        List<String> excludeList, ParamIoMode ioMode) throws IOException {
        if (ioMode == ParamIoMode.NODB) {
            throw new IllegalStateException("Cannot use NODB option with parameter exports");
        }
        List<ParameterSetDescriptor> entries = new LinkedList<>();
        ParameterSetCrud parameterCrud = new ParameterSetCrud();
        List<ParameterSet> parameters = parameterCrud.retrieveLatestVersions();

        for (ParameterSet paramSet : parameters) {
            String name = paramSet.getName().getName();
            if (excludeList != null && excludeList.contains(name)) {
                // skip
                log.debug("Skipping: " + name + " because it's on the exclude list");
                continue;
            }

            BeanWrapper<Parameters> bean = paramSet.getParameters();
            String className = "";
            try {
                className = bean.getClazz().getName();
            } catch (PipelineException e) {
                // skip
                log.info(
                    "Skipping: " + name + " because the class no longer exists :" + e.getMessage());
                continue;
            }

            ParameterSetDescriptor parameterSetDescriptor = new ParameterSetDescriptor(name,
                className);
            parameterSetDescriptor.setLibraryParamSet(paramSet);
            parameterSetDescriptor.setState(ParameterSetDescriptor.State.EXPORT);
            entries.add(parameterSetDescriptor);
        }

        if (ioMode == ParamIoMode.STANDARD) {
            log.debug("Writing parameter library export XML file");
            try {
                ParameterLibrary library = new ParameterLibrary();
                library.setDatabaseParameterSets(entries);
                xmlManager.marshal(library, new File(destinationPath));
            } catch (DatatypeConfigurationException | JAXBException e) {
                throw new PipelineException(
                    "Unable to export parameter library to file " + destinationPath, e);
            }
        }

        return entries;
    }

    /**
     * Imports the contents of the specified file or directory into the parameter library.
     * Directories are recursed in-order. Parameter sets in the library whose name matches an entry
     * in the exclude list will not be imported. If the {@code dryRun} flag is {@code true}, the
     * library will not be modified, but the {@code ParameterSetDescriptor} will be populated and
     * the state will indicate the operation that would have taken effect if {@code dryRun} had been
     * set to {@code false}.
     *
     * @param sourceFile the file or directory to import
     * @param excludeList contains a list of parameter set names which should not be imported
     * @param ioMode indicates whether execution is to be a full execution, a dry run (which
     * requires database access), or a database-free execution.
     * @return list of {@link ParameterSetDescriptor}s
     * @throws IOException if there were problems reading the parameter files
     */
    public List<ParameterSetDescriptor> importParameterLibrary(File sourceFile,
        List<String> excludeList, ParamIoMode ioMode) throws Exception {
        List<ParameterSetDescriptor> results = null;

        if (sourceFile.isDirectory()) {
            results = new ArrayList<>();

            // Skip subversion directories.
            if (sourceFile.getName().equals(".svn")) {
                return results;
            }

            // Load all of the .xml files in the directory in lexicographic
            // order. Recurse directories in-order.
            File[] files = sourceFile.listFiles((FilenameFilter) (dir,
                name) -> name.endsWith(".xml") || new File(dir, name).isDirectory());
            Arrays.sort(files);
            for (File file : files) {
                if (file.isDirectory()) {
                    results.addAll(importParameterLibrary(file, excludeList, ioMode));
                } else {
                    results.addAll(
                        importParameterLibrary(file.getAbsolutePath(), excludeList, ioMode));
                }
            }
        } else {
            results = importParameterLibrary(sourceFile.getAbsolutePath(), excludeList, ioMode);
        }

        return results;
    }

    /**
     * Import the contents of the specified directory into the parameter library. Parameter sets in
     * the library whose name matches an entry in the exclude list will not be imported. If the
     * dryRun flag is true, the library will not be modified, but the ParameterImportResults will be
     * populated and the state will indicate the operation that would have taken effect if dryRun
     * was set to true.
     *
     * @param sourcePath
     * @param excludeList Will not be imported
     * @param ioMode indicates whether execution is to be a full execution, a dry run (which
     * requires database access), or a database-free execution. changed.
     * @return
     * @throws IOException
     */
    public List<ParameterSetDescriptor> importParameterLibrary(String sourcePath,
        List<String> excludeList, ParamIoMode ioMode) throws Exception {
        ParameterSetCrud paramCrud = new ParameterSetCrud();
        ParameterLibrary library = xmlManager.unmarshal(new File(sourcePath));
        List<ParameterSetDescriptor> entries = library.getParameterSetDescriptors();
        if (library.isOverrideOnly()) {
            for (ParameterSetDescriptor desc : entries) {
                ParameterSet currentParamSet = paramCrud
                    .retrieveLatestVersionForName(desc.getName());
                if (currentParamSet == null) {
                    throw new PipelineException("OverrideOnly is true, but " + desc.getName()
                        + " does not exist in the parameter library");
                }
            }
        }

        int importCount = 0;

        for (ParameterSetDescriptor desc : entries) {
            String name = desc.getName();

            if (skipParameterSetDescriptor(desc, excludeList)) {
                continue;
            }

            // Remove confusing whitespace around values in the imported parameters
            desc.getImportedParamsBean().trimWhitespace();
            importCount += 1;

            ParameterSet currentParamSet = null;
            if (ioMode != ParamIoMode.NODB) {
                currentParamSet = paramCrud.retrieveLatestVersionForName(name);
            }

            // Set the ParameterSetDescriptor with the information about which properties
            // come from the file
            desc.setFileProps(formatProps(desc.getImportedProperties()));

            // A few different options for what we can do now:
            // Can't perform an override if the parameter set isn't in the library yet
            if (currentParamSet == null && library.isOverrideOnly()) {
                throw new UnsupportedOperationException("Cannot apply overrides to parameter set "
                    + name + " due to absence of parameter set in database");
            }
            if (currentParamSet == null) {
                // If there's no parameter set in the database, create one
                createNewParameterSet(desc, ioMode);
            } else if (library.isOverrideOnly()) {
                // If the user is performing overrides, merge the parameter sets and store them
                overrideExistingParameters(desc, currentParamSet, ioMode);
            } else {
                // Otherwise, replace an existing parameter set
                replaceExistingParameters(desc, currentParamSet, ioMode);
            }

        } // end of loop over entries from file

        log.info("Imported " + importCount + " parameter sets from: " + sourcePath);

        // Get any parameter sets that are in the library but not in this import and add them
        // to the entries.
        addLibraryOnlyParameterSets(entries, ioMode);
        return entries;
    }

    /**
     * Creates a new named parameter set in the database.
     */
    private void createNewParameterSet(ParameterSetDescriptor desc, ParamIoMode ioMode) {

        // Make certain that the ParameterSetDescriptor is valid
        desc.validate();

        String name = desc.getName();
        desc.setState(ParameterSetDescriptor.State.CREATE);

        // Construct an instance of the Parameters class that has all the fields
        log.debug("debug: " + name + ", not in parameter library, create needed");
        if (ioMode == ParamIoMode.STANDARD) {
            ParameterSet newParamSet = new ParameterSet(name);
            newParamSet.setDescription("Created by importParameterLibrary @ " + new Date());
            newParamSet.setParameters(desc.getFullParametersBean());
            new ParameterSetCrud().create(newParamSet);
        }
    }

    /**
     * Applies overrides to an existing parameter set. In this case, the parameter values in the
     * {@link ParameterSetDescriptor} overwrite parameter values in the database, but any parameters
     * that are not specified in the {@link ParameterSetDescriptor} keep their database values.
     */
    private void overrideExistingParameters(ParameterSetDescriptor desc,
        ParameterSet databaseParameterSet, ParamIoMode ioMode) {

        // Here we need to validate the descriptor against the fields of the database parameter
        // set, and we can't yet validate the primitive types of the typed properties:
        desc.validateAgainstParameterSet(databaseParameterSet);

        // Next we need to create a new ParameterSet instance that's a copy of the
        // database instance and copy new values into it
        ParameterSet mergedParameterSet = new ParameterSet(databaseParameterSet);
        Set<TypedParameter> libraryProperties = new HashSet<>();

        // Make maps from the property names to the two sets of properties (imported and merged)
        Map<String, TypedParameter> importedProperties = nameToTypedPropertyMap(
            desc.getImportedProperties());
        Map<String, TypedParameter> mergedProperties = nameToTypedPropertyMap(
            mergedParameterSet.getParameters().getTypedProperties());

        // Find the merged properties that correspond to the imported ones and copy over the
        // imported value
        for (String name : importedProperties.keySet()) {
            mergedProperties.get(name).setValue(importedProperties.get(name).getValue());
        }

        // Find the merged properties that have no corresponding imported ones and capture them
        // as library properties
        for (String name : mergedProperties.keySet()) {
            if (!importedProperties.containsKey(name)) {
                libraryProperties.add(mergedProperties.get(name));
            }
        }
        desc.setLibraryProps(formatProps(libraryProperties));

        // Now we can validate the primitive types of the merged properties
        if (!mergedParameterSet.getParameters().checkPrimitiveDataTypes()) {
            throw new IllegalStateException("Parameter set descriptor " + desc.getName()
                + " typed property values do not match expected types");
        }

        // If the parameter values have changed, commit the new parameters to the database
        desc.setState(
            updateParameters(databaseParameterSet, mergedParameterSet.getParameters(), ioMode));
    }

    Map<String, TypedParameter> nameToTypedPropertyMap(Set<TypedParameter> typedProperties) {
        Map<String, TypedParameter> nameToTypedProperty = new HashMap<>();
        for (TypedParameter property : typedProperties) {
            nameToTypedProperty.put(property.getName(), property);
        }
        return nameToTypedProperty;
    }

    /**
     * Performs complete replacement of an existing parameter set with a new one.
     * <p>
     * For instances of {@link DefaultParameters}, the replacement allows not only the values but
     * also the parameters themselves. The original parameters (names, types, and values) are
     * replaced. For all other classes, the current parameters of the class are used (which may
     * differ from the values in the library if the class has been modified). In either case, the
     * current library parameters are not copied to the new parameter set. For partial parameter set
     * replacement (i.e., keep some values and replace others), use parameter overrides.
     */
    private void replaceExistingParameters(ParameterSetDescriptor desc,
        ParameterSet databaseParameterSet, ParamIoMode ioMode) {

        // Make certain that the ParameterSetDescriptor is valid
        desc.validate();

        // perform updates as needed
        desc.setState(updateParameters(databaseParameterSet, desc.getFullParametersBean(), ioMode));
    }

    /**
     * Updates database parameters if changes have been made.
     *
     * @param databaseParameterSet parameter set from the database
     * @param newParameters BeanWrapper of new parameters
     * @param ioMode
     * @return state SAME if no update was performed, state UPDATE if updates were made.
     */
    private ParameterSetDescriptor.State updateParameters(ParameterSet databaseParameterSet,
        BeanWrapper<Parameters> newParameters, ParamIoMode ioMode) {
        // If the parameter values have changed, commit the new parameters to the database
        PipelineOperations pipelineOps = new PipelineOperations();
        if (pipelineOps.compareParameters(databaseParameterSet.getParameters(), newParameters)) {
            log.debug("name: " + databaseParameterSet.getName().getName()
                + " contents match parameter library, no update needed");
            return ParameterSetDescriptor.State.SAME;
        }
        log.debug("name: " + databaseParameterSet.getName()
            + " contents do not match parameter library, update needed");
        if (ioMode == ParamIoMode.STANDARD) {
            pipelineOps.updateParameterSet(databaseParameterSet, newParameters.getInstance(),
                false);
        }
        return ParameterSetDescriptor.State.UPDATE;
    }

    private boolean skipParameterSetDescriptor(ParameterSetDescriptor desc,
        List<String> excludeList) {
        String name = desc.getName();
        if (excludeList != null && excludeList.contains(name)) {
            // skip
            log.debug("Skipping: " + name + " because it's on the exclude list");
            desc.setState(ParameterSetDescriptor.State.IGNORE);
            return true;
        }

        if (desc.getState() == ParameterSetDescriptor.State.CLASS_MISSING) {
            // skip
            log.info("Skipping: " + name + " because the class (" + desc.getClassName()
                + ") was not found on the classpath");
            return true;
        }
        return false;
    }

    private void addLibraryOnlyParameterSets(List<ParameterSetDescriptor> entries,
        ParamIoMode ioMode) {
        List<String> importNames = new LinkedList<>();
        for (ParameterSetDescriptor importDesc : entries) {
            importNames.add(importDesc.getName());
        }

        if (ioMode != ParamIoMode.NODB) {
            List<ParameterSet> allLibraryEntries = new ParameterSetCrud().retrieveLatestVersions();
            for (ParameterSet libraryParamSet : allLibraryEntries) {
                String name = libraryParamSet.getName().getName();

                if (!importNames.contains(name)) {
                    BeanWrapper<Parameters> libraryParameters = libraryParamSet.getParameters();
                    String className = "";
                    try {
                        className = libraryParameters.getClazz().getName();
                    } catch (PipelineException e) {
                        continue;
                    }
                    ParameterSetDescriptor newDesc = new ParameterSetDescriptor(name, className);
                    newDesc.setState(ParameterSetDescriptor.State.LIBRARY_ONLY);
                    newDesc.setLibraryProps(formatProps(libraryParameters.getTypedProperties()));
                    entries.add(newDesc);
                }
            }
        }
    }

    private String formatProps(Set<TypedParameter> typedProperties) {
        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        for (TypedParameter typedProperty : typedProperties) {
            report.append("  " + typedProperty.getName() + " = " + typedProperty.getValue() + " ("
                + typedProperty.getType().toString() + ")" + nl);
        }
        return report.toString();
    }
}
