package gov.nasa.ziggy.module;

import java.io.File;

public interface AlgorithmLifecycle {

    /**
     * Return the location of the task directory, which is guaranteed to exist.
     *
     * @return
     */
    File getTaskDir(boolean cleanExisting);

    /**
     * Execute the algorithm either as a local executable or as a remote job
     *
     * @param inputs
     */
    void executeAlgorithm(TaskConfigurationManager inputs);

    /**
     * Currently generateMemdroneCacheFiles() and doTaskFileCopy().
     */
    void doPostProcessing();

    /**
     * Indicates whether the task is executing locally or remotely.
     *
     * @return
     */
    boolean isRemote();

    AlgorithmExecutor getExecutor();
}
