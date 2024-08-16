package gov.nasa.ziggy.pipeline.xml;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Utility functions for importing and exporting the parameter library.
 *
 * @author PT
 * @author Bill Wohler
 */
public class ParameterImportExportOperations extends DatabaseOperations {
    private static final Logger log = LoggerFactory
        .getLogger(ParameterImportExportOperations.class);

    private ParametersOperations parametersOperations = new ParametersOperations();
    private ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
    private ValidatingXmlManager<ParameterLibrary> xmlManager;

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
     * requires database access), or a database-free execution
     * @return list of {@link ParameterSetDescriptor}s
     */
    public List<ParameterSetDescriptor> importParameterLibrary(File sourceFile,
        List<String> excludeList, ParamIoMode ioMode) {
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
     * @param sourcePath path to the directory to import
     * @param excludeList will not be imported
     * @param ioMode indicates whether execution is to be a full execution, a dry run (which
     * requires database access), or a database-free execution. changed
     * @return
     */
    public List<ParameterSetDescriptor> importParameterLibrary(String sourcePath,
        List<String> excludeList, ParamIoMode ioMode) {

        ParameterLibrary library = xmlManager().unmarshal(new File(sourcePath));
        List<ParameterSetDescriptor> entries = library.getParameterSetDescriptors();
        if (library.isOverrideOnly()) {
            for (ParameterSetDescriptor desc : entries) {
                ParameterSet currentParamSet = performTransaction(
                    () -> parameterSetCrud().retrieveLatestVersionForName(desc.getName()));
                if (currentParamSet == null) {
                    throw new PipelineException("OverrideOnly is true, but " + desc.getName()
                        + " does not exist in the parameter library");
                }
            }
        }

        int importCount = 0;

        for (ParameterSetDescriptor desc : entries) {
            if (skipParameterSetDescriptor(desc, excludeList)) {
                continue;
            }

            String name = desc.getName();
            ParameterSet currentParamSet = null;
            importCount++;
            if (ioMode != ParamIoMode.NODB) {
                currentParamSet = performTransaction(
                    () -> parameterSetCrud().retrieveLatestVersionForName(name));
            }

            // Set the ParameterSetDescriptor with the information about which properties
            // come from the file.
            desc.setFileProps(formatProps(desc.getImportedProperties()));

            // A few different options for what we can do now:
            // Can't perform an override if the parameter set isn't in the library yet.
            if (currentParamSet == null && library.isOverrideOnly()) {
                throw new UnsupportedOperationException("Cannot apply overrides to parameter set "
                    + name + " due to absence of parameter set in database");
            }
            if (currentParamSet == null) {
                // If there's no parameter set in the database, create one.
                createNewParameterSet(desc, ioMode);
            } else if (library.isOverrideOnly()) {
                // If the user is performing overrides, merge the parameter sets and store them.
                overrideExistingParameters(currentParamSet, desc, ioMode);
            } else {
                // Otherwise, replace an existing parameter set.
                replaceExistingParameters(currentParamSet, desc, ioMode);
            }
        }

        log.info("Imported {} parameter sets from: {}", importCount, sourcePath);

        // Get any parameter sets that are in the library but not in this import and add them
        // to the entries.
        addLibraryOnlyParameterSets(entries, ioMode);

        return entries;
    }

    private boolean skipParameterSetDescriptor(ParameterSetDescriptor desc,
        List<String> excludeList) {
        String name = desc.getName();
        if (excludeList != null && excludeList.contains(name)) {
            log.debug("Skipping {} because it's on the exclude list", name);
            desc.setState(ParameterSetDescriptor.State.IGNORE);
            return true;
        }

        return false;
    }

    /**
     * Creates a new named parameter set in the database.
     */
    private void createNewParameterSet(ParameterSetDescriptor desc, ParamIoMode ioMode) {

        // Make certain that the ParameterSetDescriptor is valid.
        desc.validate();

        String name = desc.getName();
        desc.setState(ParameterSetDescriptor.State.CREATE);

        // Construct an instance of the Parameters class that has all the fields.
        log.debug("Adding {} as it is not yet in parameter library", name);
        if (ioMode == ParamIoMode.STANDARD) {
            ParameterSet newParamSet = new ParameterSet(name);
            newParamSet.setDescription("Created by importParameterLibrary @ " + new Date());
            newParamSet.setModuleInterfaceName(desc.getModuleInterfaceName());
            newParamSet.setParameters(desc.getImportedProperties());
            performTransaction(() -> parameterSetCrud().merge(newParamSet));
        }
    }

    /**
     * Applies overrides to an existing parameter set. In this case, the parameter values in the
     * {@link ParameterSetDescriptor} overwrite parameter values in the database, but any parameters
     * that are not specified in the {@link ParameterSetDescriptor} keep their database values.
     */
    private void overrideExistingParameters(ParameterSet databaseParameterSet,
        ParameterSetDescriptor desc, ParamIoMode ioMode) {

        // Here we need to validate the descriptor against the fields of the database parameter
        // set, and we can't yet validate the primitive types of the typed properties:
        desc.validateAgainstParameterSet(databaseParameterSet);

        // Next we need to create a new ParameterSet instance that's a copy of the
        // database instance and copy new values into it.
        ParameterSet mergedParameterSet = databaseParameterSet.newInstance();
        mergedParameterSet.setParameters(databaseParameterSet.copyOfParameters());
        Set<Parameter> libraryProperties = new HashSet<>();

        // Make maps from the property names to the two sets of properties (imported and merged).
        Map<String, Parameter> importedProperties = parameterOperations()
            .nameToTypedPropertyMap(desc.getImportedProperties());
        Map<String, Parameter> mergedProperties = parameterOperations()
            .nameToTypedPropertyMap(mergedParameterSet.getParameters());

        // Find the merged properties that correspond to the imported ones and copy over the
        // imported value.
        for (String name : importedProperties.keySet()) {
            mergedProperties.get(name).setString(importedProperties.get(name).getString());
        }

        // Find the merged properties that have no corresponding imported ones and capture them
        // as library properties
        for (String name : mergedProperties.keySet()) {
            if (!importedProperties.containsKey(name)) {
                libraryProperties.add(mergedProperties.get(name));
            }
        }
        desc.setLibraryProps(formatProps(libraryProperties));

        // If the parameter values have changed, commit the new parameters to the database
        desc.setState(updateParameters(databaseParameterSet, mergedParameterSet, ioMode));
    }

    /**
     * Updates database parameters if changes have been made.
     *
     * @param databaseParameterSet parameter set from the database
     * @param newParameters new parameters
     * @param ioMode
     * @return state SAME if no update was performed, state UPDATE if updates were made
     */
    public ParameterSetDescriptor.State updateParameters(ParameterSet databaseParameterSet,
        ParameterSet newParameters, ParamIoMode ioMode) {

        // If the parameter values have changed, commit the new parameters to the database
        if (Parameter.identicalParameters(databaseParameterSet.getParameters(),
            newParameters.getParameters())) {
            log.debug("The contents of {} match parameter library, no update needed",
                databaseParameterSet.getName());
            return ParameterSetDescriptor.State.SAME;
        }
        log.debug("The contents of {} do not match parameter library, no update needed",
            databaseParameterSet.getName());
        if (ioMode == ParamIoMode.STANDARD) {
            parameterOperations().updateParameterSet(databaseParameterSet, newParameters, false);
        }
        return ParameterSetDescriptor.State.UPDATE;
    }

    /**
     * Performs complete replacement of an existing parameter set with a new one.
     * <p>
     * The replacement allows not only the values but also the parameters themselves. The original
     * parameters (names, types, and values) are replaced. The current library parameters are not
     * copied to the new parameter set. For partial parameter set replacement (i.e., keep some
     * values and replace others), use parameter overrides.
     */
    private void replaceExistingParameters(ParameterSet databaseParameterSet,
        ParameterSetDescriptor desc, ParamIoMode ioMode) {

        // Make certain that the ParameterSetDescriptor is valid.
        desc.validate();

        // perform updates as needed.
        desc.setState(updateParameters(databaseParameterSet, desc.getParameterSet(), ioMode));
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void addLibraryOnlyParameterSets(List<ParameterSetDescriptor> entries,
        ParamIoMode ioMode) {

        List<String> importNames = new LinkedList<>();
        for (ParameterSetDescriptor importDesc : entries) {
            importNames.add(importDesc.getName());
        }

        if (ioMode != ParamIoMode.NODB) {
            List<ParameterSet> allLibraryEntries = performTransaction(
                () -> parameterSetCrud().retrieveLatestVersions());
            for (ParameterSet libraryParamSet : allLibraryEntries) {
                String name = libraryParamSet.getName();

                if (!importNames.contains(name)) {
                    ParameterSetDescriptor newDesc = new ParameterSetDescriptor(libraryParamSet);
                    newDesc.setState(ParameterSetDescriptor.State.LIBRARY_ONLY);
                    newDesc.setLibraryProps(formatProps(libraryParamSet.getParameters()));
                    entries.add(newDesc);
                }
            }
        }
    }

    private String formatProps(Set<Parameter> typedProperties) {
        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        for (Parameter typedProperty : typedProperties) {
            report.append("  ")
                .append(typedProperty.getName())
                .append(" = ")
                .append(typedProperty.getValue())
                .append(" (")
                .append(typedProperty.getType().toString())
                .append(")")
                .append(nl);
        }
        return report.toString();
    }

    /**
     * Export the current parameter library to the specified file.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public List<ParameterSetDescriptor> exportParameterLibrary(String destinationPath,

        List<String> excludeList, ParamIoMode ioMode) {
        if (ioMode == ParamIoMode.NODB) {
            throw new IllegalStateException("Cannot use NODB option with parameter exports");
        }

        List<ParameterSetDescriptor> entries = new LinkedList<>();
        List<ParameterSet> parameters = performTransaction(
            () -> parameterSetCrud().retrieveLatestVersions());

        for (ParameterSet paramSet : parameters) {
            String name = paramSet.getName();
            if (excludeList != null && excludeList.contains(name)) {
                log.debug("Skipping {} because it's on the exclude list", name);
                continue;
            }
            paramSet.populateXmlFields();
            ParameterSetDescriptor parameterSetDescriptor = new ParameterSetDescriptor(paramSet);
            parameterSetDescriptor.setState(ParameterSetDescriptor.State.EXPORT);
            entries.add(parameterSetDescriptor);
        }

        if (ioMode == ParamIoMode.STANDARD) {
            log.debug("Writing parameter library export XML file");
            ParameterLibrary library = new ParameterLibrary();
            library.setDatabaseParameterSets(entries);
            xmlManager().marshal(library, new File(destinationPath));
        }
        return entries;
    }

    /**
     * Instantiates the XML manager when first used. This must be done here rather than in the
     * constructor so that the constructor can run successfully in test contexts.
     */
    private ValidatingXmlManager<ParameterLibrary> xmlManager() {
        if (xmlManager == null) {
            xmlManager = new ValidatingXmlManager<>(ParameterLibrary.class);
        }
        return xmlManager;
    }

    ParametersOperations parameterOperations() {
        return parametersOperations;
    }

    ParameterSetCrud parameterSetCrud() {
        return parameterSetCrud;
    }
}
