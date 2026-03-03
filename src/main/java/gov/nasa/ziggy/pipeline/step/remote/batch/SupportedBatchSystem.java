package gov.nasa.ziggy.pipeline.step.remote.batch;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.pipeline.step.remote.BatchManager;
import gov.nasa.ziggy.pipeline.step.remote.BatchParameters;
import gov.nasa.ziggy.pipeline.step.remote.BatchParametersAggregator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Batch systems that are supported by Ziggy.
 * <p>
 * The "supported" batch systems includes NONE, which is what you get back if there are no defined
 * batch systems and/or the {@link #getInstance()} method finds that it's running outside the
 * context of a batch system. Use {@link #validValues()} to get just the list of values that are
 * actual batch systems and not NONE. If you add additional invalid entries to SupportedBatchSystem,
 * add an additional filter term in that method to skip over your new invalid entry.
 */
public enum SupportedBatchSystem {

    PBS(PbsBatchParameters.class, new PbsBatchManager(), new PbsBatchParametersAggregator(),
        "pbs") {

        @Override
        public boolean isBatchSystem() {
            return !StringUtils.isBlank(System.getenv("PBS_JOBID"));
        }

        @Override
        public String jobId() {
            String fullJobId = System.getenv("PBS_JOBID");
            return fullJobId.split("\\.")[0];
        }

        @Override
        public String jobName() {
            return System.getenv("PBS_JOBNAME");
        }
    },
    NONE(null, null, null, null) {
        @Override
        public boolean isBatchSystem() {
            return false;
        }

        @Override
        public String jobId() {
            return "none";
        }

        @Override
        public String jobName() {
            return "none";
        }

        @Override
        public BatchManager<? extends BatchParameters> batchManager() {
            throw new UnsupportedOperationException("No batch system specified");
        }

        @Override
        public BatchParametersAggregator<? extends BatchParameters> batchParametersAggregator() {
            throw new UnsupportedOperationException("No batch system specified");
        }

        @Override
        public BatchParameters batchParameters() {
            throw new UnsupportedOperationException("No batch system specified");
        }

        @Override
        public String logFileRelativePath() {
            throw new UnsupportedOperationException("No batch system specified");
        }
    };

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

    /** Determines whether a compute node is using the given batch system. */
    public abstract boolean isBatchSystem();

    /** Returns the ID of a given job, as a String. */
    public abstract String jobId();

    /** Returns the name of a given job, as a String. */
    public abstract String jobName();

    /** Returns the batch system in use for a given job, or NONE if running locally. */
    public static SupportedBatchSystem getInstance() {
        for (SupportedBatchSystem supportedBatchSystem : values()) {
            if (supportedBatchSystem.isBatchSystem()) {
                return supportedBatchSystem;
            }
        }
        return SupportedBatchSystem.NONE;
    }

    /**
     * Returns only valid {@link SupportedBatchSystem} instances that aren't NONE.
     */
    public static List<SupportedBatchSystem> validValues() {
        return List.of(values()).stream().filter(e -> e != NONE).toList();
    }
}
