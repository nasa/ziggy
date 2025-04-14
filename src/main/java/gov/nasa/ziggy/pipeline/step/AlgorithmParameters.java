package gov.nasa.ziggy.pipeline.step;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.step.io.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.util.io.Persistable;

/**
 * Container class for algorithm parameters. The {@link DatastoreDirectoryPipelineInputs} class uses
 * a {@link AlgorithmParameters} instance to store all Parameters instances. This facilitates
 * special handling for same by the HDF5 algorithm interface infrastructure.
 *
 * @author PT
 */
public class AlgorithmParameters implements Persistable {

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
