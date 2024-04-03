package gov.nasa.ziggy.module;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allocates subtasks to clients that execute them in the order specified by an
 * {@link TaskConfiguration} instance.
 * <p>
 * This class is typically accessed over a socket using {@link SubtaskServer} and
 * {@link SubtaskClient}
 *
 * @author Todd Klaus
 */
public class SubtaskAllocator {
    private static final Logger log = LoggerFactory.getLogger(SubtaskAllocator.class);

    LinkedList<Integer> currentPoolWaiting = new LinkedList<>();
    List<Integer> currentPoolProcessing = new LinkedList<>();
    boolean[] subtaskCompleted;

    @Override
    public String toString() {
        return "ag:[currentPoolWaiting=" + currentPoolWaiting + ", currentPoolProcessing="
            + currentPoolProcessing + "]";
    }

    public SubtaskAllocator(TaskConfiguration taskConfiguration) {
        if (taskConfiguration.getSubtaskCount() > 0) {
            subtaskCompleted = new boolean[taskConfiguration.getSubtaskCount()];
            populateWaitingPool();
        }
    }

    public boolean markSubtaskComplete(int subTaskIndex) {
        boolean found = markSubtaskNeedsNoFurtherProcessing(subTaskIndex);
        subtaskCompleted[subTaskIndex] = true;
        return found;
    }

    public boolean markSubtaskLocked(int subTaskIndex) {
        return markSubtaskNeedsNoFurtherProcessing(subTaskIndex);
    }

    private boolean markSubtaskNeedsNoFurtherProcessing(int subTaskIndex) {
        boolean found = false;
        for (int i = 0; i < currentPoolProcessing.size(); i++) {
            if (currentPoolProcessing.get(i) == subTaskIndex) {
                found = true;
                currentPoolProcessing.remove(i);
                log.debug("removing subtaskIndex: " + subTaskIndex);
            }
        }
        if (!found) {
            log.warn("failed to remove subtaskIndex: " + subTaskIndex);
            return false;
        }
        return true;
    }

    /**
     * Return the next sub-task available for processing
     *
     * @return
     */
    public SubtaskAllocation nextSubtask() {

        // If the last subtask is processing or complete, see whether it's necessary
        // to go through the subtasks again looking for orphans
        handleEndOfSubtasks();

        // If no orphans, we can signal that this job is done
        if (lastSubtaskProcessingOrComplete()) {
            return new SubtaskAllocation(SubtaskServer.ResponseType.NO_MORE, -1);
        }

        // Otherwise, get the next available subtask from waiting and move to processing
        int subtaskIndex = -1;
        boolean doneLookingForNextSubtask = false;
        while (!doneLookingForNextSubtask) {
            subtaskIndex = currentPoolWaiting.remove();
            doneLookingForNextSubtask = !subtaskCompleted[subtaskIndex]
                && !currentPoolProcessing.contains(subtaskIndex);
            if (!doneLookingForNextSubtask && currentPoolWaiting.isEmpty()) {
                // If we got this far, then all the remaining subtasks that are not yet
                // complete are currently processing in this job. In that case we can
                // tell the subtask server that it should check in again when either the
                // subtasks are completed or we know that they're locked by another job.
                return new SubtaskAllocation(SubtaskServer.ResponseType.TRY_AGAIN, -1);
            }
        }
        currentPoolProcessing.add(subtaskIndex);
        return new SubtaskAllocation(SubtaskServer.ResponseType.OK, subtaskIndex);
    }

    private boolean lastSubtaskProcessingOrComplete() {
        return currentPoolWaiting.isEmpty();
    }

    private void handleEndOfSubtasks() {
        // we only need to do anything if the last subtask is processing or even completed, and
        // there are subtasks that were found to be locked when offered for processing
        if (!lastSubtaskProcessingOrComplete()) {
            return;
        }
        boolean allSubtasksComplete = true;
        for (boolean subtaskComplete : subtaskCompleted) {
            allSubtasksComplete = allSubtasksComplete && subtaskComplete;
        }
        if (allSubtasksComplete) {
            return;
        }

        // If there are subtasks that were locked, they might not have gotten processed because
        // the job that was processing them might have timed out while subtasks were still
        // running. In that case we need to go back to the beginning and process any subtasks
        // that are not currently locked but are shown as processing (these are the subtasks
        // that got stranded when a job timed out).
        populateWaitingPool();

        // Anything that's already in the processing pool can go out of the waiting pool
        removeProcessingPoolFromWaitingPool();
    }

    private void removeProcessingPoolFromWaitingPool() {
        if (currentPoolProcessing.isEmpty()) {
            return;
        }
        for (int subtaskIndex : currentPoolProcessing) {
            int locationInWaitingPool = currentPoolWaiting.indexOf(subtaskIndex);
            if (locationInWaitingPool > -1) {
                currentPoolWaiting.remove(locationInWaitingPool);
            }
        }
    }

    private void populateWaitingPool() {
        for (int i = 0; i < subtaskCompleted.length; i++) {
            currentPoolWaiting.add(i);
        }
    }

    public boolean isEmpty() {
        return subtaskCompleted == null || subtaskCompleted.length == 0;
    }
}
