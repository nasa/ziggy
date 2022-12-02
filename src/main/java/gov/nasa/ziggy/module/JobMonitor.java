package gov.nasa.ziggy.module;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import gov.nasa.ziggy.module.remote.QstatMonitor;
import gov.nasa.ziggy.module.remote.QueueCommandManager;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Interface for classes that monitor remote jobs. This allows a dummy implementation to be supplied
 * in cases in which there are calls to a remote job monitor but no remote jobs to be monitored (see
 * {@link AlgorithmMonitor} for more information).
 *
 * @author PT
 */
public interface JobMonitor {

    /**
     * Returns a new instance of {@link JobMonitor} that is correct for its use-case based on
     * arguments. In particular, for remote tasks an instance of {@link QstatMonitor} will be
     * returned, while for local tasks a dummy instance, with no actual functionality, will be
     * returned.
     */
    static JobMonitor newInstance(String username, boolean remoteJobs) {
        if (remoteJobs) {
            return new QstatMonitor(username, System.getenv("HOST"));
        } else {
            return new JobMonitor() {
            };
        }
    }

    default void addToMonitoring(StateFile stateFile) {

    }

    default void addToMonitoring(PipelineTask pipelineTask) {

    }

    default void endMonitoring(StateFile stateFile) {

    }

    default void update() {

    }

    default Set<Long> allIncompleteJobIds(PipelineTask pipelineTask) {
        return Collections.emptySet();
    }

    default Set<Long> allIncompleteJobIds(StateFile stateFile) {
        return Collections.emptySet();
    }

    default boolean isFinished(StateFile stateFile) {
        return false;
    }

    default Map<Long, Integer> exitStatus(StateFile stateFile) {
        return Collections.emptyMap();
    }

    default Map<Long, String> exitComment(StateFile stateFile) {
        return Collections.emptyMap();
    }

    default String getOwner() {
        return new String();
    }

    default String getServerName() {
        return new String();
    }

    default QueueCommandManager getQstatCommandManager() {
        return null;
    }

    default Set<String> getJobsInMonitor() {
        return Collections.emptySet();
    }

}
