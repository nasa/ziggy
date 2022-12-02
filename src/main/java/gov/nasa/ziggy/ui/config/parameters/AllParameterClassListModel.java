package gov.nasa.ziggy.ui.config.parameters;

import gov.nasa.ziggy.ui.ops.parameters.ParametersClassCache;

@SuppressWarnings("serial")
public class AllParameterClassListModel extends ParameterClassListModel {
    public AllParameterClassListModel() throws Exception {
        // super(new LinkedList<ClassWrapper<Parameters>>());
        super(ParametersClassCache.getCache());
    }
}
