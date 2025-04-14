package gov.nasa.ziggy.pipeline.definition.importer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;

/**
 * Performs conditioning of imported parameter sets to a state that can be persisted to the
 * database. Conditioning consists of the following:
 * <ol>
 * <li>Apply partial updates to parameter sets in the database.
 * <li>Identify parameter sets that are new (i.e., no matching set in the database).
 * <li>Identify parameter sets that are updated.
 * <li>Identify parameter sets for which the import and the existing parameter set are identical.
 * <li>Retrieve parameter sets from the database that are not referenced in the imports.
 * </ol>
 *
 * @author PT
 */
public class ParameterSetImportConditioner {

    private static final Logger log = LoggerFactory.getLogger(ParameterSetImportConditioner.class);

    private ParametersOperations parametersOperations = new ParametersOperations();

    /**
     * Constructs {@link ParameterSetDescriptor}s for a collection of {@link ParameterSet}
     * instances. Each parameter set descriptor includes information about whether its parameter set
     * is new or represents an update of a parameter set in the database.
     *
     * @param parameterSets
     * @param update if true, allow updates to existing parameter sets
     */
    public List<ParameterSetDescriptor> parameterSetDescriptors(List<ParameterSet> parameterSets,
        boolean update) {
        int importCount = 0;
        List<ParameterSetDescriptor> entries = new ArrayList<>();
        for (ParameterSet parameterSet : parameterSets) {
            ParameterSetDescriptor descriptor = new ParameterSetDescriptor(parameterSet);
            parameterSet.populateDatabaseFields();
            entries.add(descriptor);
            importCount++;

            ParameterSet currentParamSet = parametersOperations().parameterSet(descriptor.getName());

            // Set the ParameterSetDescriptor with the information about which properties
            // come from the file.
            descriptor.setFileProps(formatProps(descriptor.getImportedProperties()));

            if (currentParamSet == null) {

                // New parameter set.
                descriptor.setState(ParameterSetDescriptor.State.CREATE);
            } else if (update) {

                // Parameter sets that may require an update.
                descriptor.setState(descriptorState(currentParamSet, descriptor));
            } else {

                // If we're not updating, the state has to be SAME.
                descriptor.setState(ParameterSetDescriptor.State.SAME);
            }
        }
        log.info("Imported {} parameter sets", importCount);
        addLibraryParameterSets(entries);

        return entries;
    }

    public static String formatProps(Set<Parameter> parameters) {
        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        for (Parameter typedProperty : parameters) {
            report.append("  ").append(typedProperty.toString()).append(nl);
        }
        return report.toString();
    }

    private ParameterSetDescriptor.State descriptorState(ParameterSet databaseParameterSet,
        ParameterSetDescriptor descriptor) {
        ParameterSet newParameters = descriptor.getParameterSet().isPartial()
            ? mergeParameterSets(databaseParameterSet, descriptor)
            : descriptor.getParameterSet();

        if (Parameter.identicalParameters(databaseParameterSet.getParameters(),
            newParameters.getParameters())) {
            log.debug("The contents of {} match parameter library, no update needed",
                databaseParameterSet.getName());
            return ParameterSetDescriptor.State.SAME;
        }
        log.debug("The contents of {} do not match parameter library, update needed",
            databaseParameterSet.getName());
        return ParameterSetDescriptor.State.UPDATE;
    }

    private ParameterSet mergeParameterSets(ParameterSet databaseParameterSet,
        ParameterSetDescriptor descriptor) {

        // Here we need to validate the descriptor against the fields of the database parameter
        // set, and we can't yet validate the primitive types of the typed properties:
        descriptor.validateAgainstParameterSet(databaseParameterSet);

        // Next we need to create a new ParameterSet instance that's a copy of the
        // database instance and copy new values into it.
        ParameterSet mergedParameterSet = databaseParameterSet.newInstance();
        mergedParameterSet.setParameters(databaseParameterSet.copyOfParameters());

        // Make maps of the parameters by name from both imported and merged parameter sets.
        Map<String, Parameter> importedParameterByName = parametersOperations()
            .nameToTypedPropertyMap(descriptor.getImportedProperties());
        Map<String, Parameter> mergedParameterByName = parametersOperations()
            .nameToTypedPropertyMap(mergedParameterSet.getParameters());

        // Find the merged properties that correspond to the imported ones and copy over the
        // imported value.
        for (String name : importedParameterByName.keySet()) {
            mergedParameterByName.get(name)
                .setString(importedParameterByName.get(name).getString());
        }
        return mergedParameterSet;
    }

    private void addLibraryParameterSets(List<ParameterSetDescriptor> entries) {

        Map<String, ParameterSetDescriptor> importDescriptorsByName = new HashMap<>();
        for (ParameterSetDescriptor importDesc : entries) {
            importDescriptorsByName.put(importDesc.getName(), importDesc);
        }

        List<ParameterSet> allLibraryEntries = parametersOperations().parameterSets();
        for (ParameterSet libraryParamSet : allLibraryEntries) {
            String name = libraryParamSet.getName();

            if (importDescriptorsByName.containsKey(name)) {
                ParameterSetDescriptor descriptor = importDescriptorsByName.get(name);
                descriptor.setLibraryProps(formatProps(libraryParamSet.getParameters()));
            } else {
                ParameterSetDescriptor newDesc = new ParameterSetDescriptor(libraryParamSet);
                newDesc.setState(ParameterSetDescriptor.State.LIBRARY_ONLY);
                newDesc.setLibraryProps(formatProps(libraryParamSet.getParameters()));
                entries.add(newDesc);
            }
        }
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }
}
