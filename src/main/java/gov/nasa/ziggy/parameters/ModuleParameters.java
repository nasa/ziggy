package gov.nasa.ziggy.parameters;

import java.util.ArrayList;
import java.util.List;

import gov.nasa.ziggy.module.io.Persistable;

/**
 * Container class for module parameters. The DefaultPipelineInputs class uses a ModuleParameters
 * instance to store all Parameters instances. This facilitates special handling for same by the
 * HDF5 module interface infrastructure.
 *
 * @author PT
 */
public class ModuleParameters implements Persistable {

    private List<ParametersInterface> moduleParameters = new ArrayList<>();

    public List<ParametersInterface> getModuleParameters() {
        return moduleParameters;
    }

    public void setModuleParameters(List<ParametersInterface> moduleParameters) {
        this.moduleParameters = moduleParameters;
    }
}
