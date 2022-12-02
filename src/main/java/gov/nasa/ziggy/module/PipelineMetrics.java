package gov.nasa.ziggy.module;

import gov.nasa.ziggy.worker.WorkerTaskRequestDispatcher;

/**
 * Metrics used by the pipeline in various contexts.
 *
 * @author PT
 */
public class PipelineMetrics {

    public static final String MATLAB_SERIALIZATION_METRIC = "pipeline.module.executeAlgorithm.matlab.serializationTime";
    public static final String MATLAB_MATFILE_METRIC = "pipeline.module.executeAlgorithm.matlab.readWriteMatfilesTime";
    public static final String MATLAB_CONTROLLER_EXEC_METRIC = "pipeline.module.executeAlgorithm.matlab.controller.execTime";

    public static final String CREATE_INPUTS_METRIC = "pi.module.matlab.createInputs.execTimeMillis";
    public static final String SEND_METRIC = "pi.module.matlab.remote.send.execTimeMillis";
    public static final String REMOTE_WORKER_WAIT_METRIC = "pi.module.matlab.waitForRemoteWorker.elapsedTimeMillis";
    public static final String PLEIADES_QUEUE_METRIC = "pi.module.matlab.pleiadesQueue.elapsedTimeMillis";
    public static final String PLEIADES_WALL_METRIC = "pi.module.matlab.pleiadesWall.elapsedTimeMillis";
    public static final String PENDING_RECEIVE_METRIC = "pi.module.matlab.waitForSoc.elapsedTimeMillis";
    public static final String RECEIVE_METRIC = "pi.module.matlab.remote.receive.execTimeMillis";
    public static final String STORE_OUTPUTS_METRIC = "pi.module.matlab.storeOutputs.execTimeMillis";
    public static final String TF_INPUTS_SIZE_METRIC = "pi.module.matlab.taskFiles.inputs.sizeBytes";
    public static final String TF_PFE_OUTPUTS_SIZE_METRIC = "pi.module.matlab.taskFiles.pleiadesOutputs.sizeBytes";
    public static final String TF_ARCHIVE_SIZE_METRIC = "pi.module.matlab.taskFiles.archive.sizeBytes";
    public static final String COPY_TASK_FILES_METRIC = "pi.module.matlab.copyTaskFiles.execTimeMillis";

    public static final String JAVA_SERIALIZATION_METRIC = "pipeline.module.executeAlgorithm.java.serializationTime";

    static final String[] REMOTE_METRICS = { CREATE_INPUTS_METRIC, SEND_METRIC,
        REMOTE_WORKER_WAIT_METRIC, PLEIADES_QUEUE_METRIC, PLEIADES_WALL_METRIC,
        PENDING_RECEIVE_METRIC, RECEIVE_METRIC, STORE_OUTPUTS_METRIC, COPY_TASK_FILES_METRIC,
        WorkerTaskRequestDispatcher.PIPELINE_MODULE_COMMIT_METRIC, TF_INPUTS_SIZE_METRIC,
        TF_PFE_OUTPUTS_SIZE_METRIC, TF_ARCHIVE_SIZE_METRIC };

    static final String[] LOCAL_METRICS = { CREATE_INPUTS_METRIC,
        SubtaskExecutor.MATLAB_PROCESS_EXEC_METRIC, STORE_OUTPUTS_METRIC, COPY_TASK_FILES_METRIC,
        WorkerTaskRequestDispatcher.PIPELINE_MODULE_COMMIT_METRIC, TF_ARCHIVE_SIZE_METRIC };

}
