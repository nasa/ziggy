package gov.nasa.ziggy.pipeline.step.remote.batch;

import java.util.Collection;

import gov.nasa.ziggy.pipeline.step.remote.BatchParameters;
import gov.nasa.ziggy.pipeline.step.remote.BatchParametersAggregator;

public class PbsBatchParametersAggregator implements BatchParametersAggregator<PbsBatchParameters> {

    @Override
    public PbsBatchParameters aggregate(Collection<BatchParameters> parametersInstances) {
        return PbsBatchParameters.aggregate(parametersInstances);
    }
}
