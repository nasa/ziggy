package gov.nasa.ziggy.module;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;

/**
 * Container class for module parameters. The {@link DatastoreDirectoryPipelineInputs} class uses a
 * {@link ModuleParameters} instance to store all Parameters instances. This facilitates special
 * handling for same by the HDF5 module interface infrastructure.
 *
 * @author PT
 */
public class ModuleParameters implements Persistable {

    private Map<String, ParameterSet> parameterSetsByName = new HashMap<>();

    public Map<String, ParameterSet> getParameterSetsByName() {
        return parameterSetsByName;
    }

    public void addParameterSets(Set<ParameterSet> parameterSets) {
        Map<String, ParameterSet> newParameterSetsByName = ParameterSet
            .parameterSetByName(parameterSets);
        parameterSetsByName.putAll(newParameterSetsByName);
    }
}
