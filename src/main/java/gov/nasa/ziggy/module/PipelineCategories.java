package gov.nasa.ziggy.module;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics.Units;

/**
 * Categories used by the pipeline in various contexts.
 *
 * @author PT
 */
public class PipelineCategories {

    public static final String CREATE_INPUTS_CATEGORY = "CreateInputs";
    public static final String SEND_INPUTS_CATEGORY = "SendInputs";
    public static final String REMOTE_WORKER_CATEGORY = "RemoteWorker";
    public static final String PLEIADES_QUEUE_CATEGORY = "PleiadesQueue";
    public static final String MATLAB_CATEGORY = "Matlab";
    public static final String PENDING_RECEIVE_CATEGORY = "PendingReceive";
    public static final String RECEIVE_OUTPUTS_CATEGORY = "ReceiveOutputs";
    public static final String STORE_OUTPUTS_CATEGORY = "StoreOutputs";
    public static final String COPY_TASK_FILES_CATEGORY = "CopyTaskFiles";
    public static final String COMMIT_CATEGORY = "Commit";

    public static final String TF_INPUTS_SIZE_CATEGORY = "InputsSize";
    public static final String TF_PFE_OUTPUTS_SIZE_CATEGORY = "PleiadesOutputsSize";
    public static final String TF_ARCHIVE_SIZE_CATEGORY = "ArchiveSize";

    static final String[] REMOTE_CATEGORIES = { CREATE_INPUTS_CATEGORY, SEND_INPUTS_CATEGORY,
        REMOTE_WORKER_CATEGORY, PLEIADES_QUEUE_CATEGORY, MATLAB_CATEGORY, PENDING_RECEIVE_CATEGORY,
        RECEIVE_OUTPUTS_CATEGORY, STORE_OUTPUTS_CATEGORY, COPY_TASK_FILES_CATEGORY, COMMIT_CATEGORY,
        TF_INPUTS_SIZE_CATEGORY, TF_PFE_OUTPUTS_SIZE_CATEGORY, TF_ARCHIVE_SIZE_CATEGORY };

    static final Units[] REMOTE_CATEGORY_UNITS = { Units.TIME, // create inputs time
        Units.TIME, // send inputs time
        Units.TIME, // remote worker time
        Units.TIME, // pleiades queue time
        Units.TIME, // matlab time
        Units.TIME, // pending receive time
        Units.TIME, // receive time
        Units.TIME, // store outputs time
        Units.TIME, // copy task files time
        Units.TIME, // commit time
        Units.BYTES, // inputs size
        Units.BYTES, // outputs size
        Units.BYTES // archive size
    };

    static final String[] LOCAL_CATEGORIES = { CREATE_INPUTS_CATEGORY, MATLAB_CATEGORY,
        STORE_OUTPUTS_CATEGORY, COPY_TASK_FILES_CATEGORY, COMMIT_CATEGORY,
        TF_ARCHIVE_SIZE_CATEGORY };

    static final Units[] LOCAL_CATEGORY_UNITS = { Units.TIME, // create inputs time
        Units.TIME, // matlab time
        Units.TIME, // store outputs time
        Units.TIME, // copy task files time
        Units.TIME, // commit time
        Units.BYTES // archive size
    };

}
