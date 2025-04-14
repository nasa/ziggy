package gov.nasa.ziggy.pipeline.step.remote.batch;

import java.lang.reflect.InvocationTargetException;

import gov.nasa.ziggy.pipeline.step.remote.BatchManager;
import gov.nasa.ziggy.pipeline.step.remote.BatchParameters;
import gov.nasa.ziggy.pipeline.step.remote.BatchParametersAggregator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;

/** Batch systems that are supported by Ziggy. */
public enum SupportedBatchSystem {

    PBS(PbsBatchParameters.class, new PbsBatchManager(), new PbsBatchParametersAggregator(), "pbs");

    private Class<? extends BatchParameters> batchParametersClass;
    private BatchManager<? extends BatchParameters> batchManager;
    private BatchParametersAggregator<? extends BatchParameters> batchParametersAggregator;
    private String logFileRelativePath;

    SupportedBatchSystem(Class<? extends BatchParameters> batchParametersClass,
        BatchManager<? extends BatchParameters> batchManager,
        BatchParametersAggregator<? extends BatchParameters> batchParametersAggregator,
        String logFileRelativePath) {
        this.batchParametersClass = batchParametersClass;
        this.batchManager = batchManager;
        this.batchParametersAggregator = batchParametersAggregator;
        this.logFileRelativePath = logFileRelativePath;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public BatchParameters batchParameters() {
        try {
            return batchParametersClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new PipelineException(
                "Unable to instantiate " + batchParametersClass.getName() + " instance", e);
        }
    }

    public BatchManager<? extends BatchParameters> batchManager() {
        return batchManager;
    }

    public BatchParametersAggregator<? extends BatchParameters> batchParametersAggregator() {
        return batchParametersAggregator;
    }

    public String logFileRelativePath() {
        return logFileRelativePath;
    }
}
