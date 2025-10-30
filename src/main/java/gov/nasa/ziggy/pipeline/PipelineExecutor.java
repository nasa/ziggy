package gov.nasa.ziggy.pipeline;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.definition.database.RuntimeObjectFactory;
import gov.nasa.ziggy.services.alert.Alert;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;
import gov.nasa.ziggy.services.messages.RemoveTaskFromKilledTasksMessage;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import gov.nasa.ziggy.worker.WorkerResources;
import gov.nasa.ziggy.worker.WorkerResourcesOperations;

/***
 * Encapsulates the launch and transition logic for pipelines.
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineExecutor {
    private static final Logger log = LoggerFactory.getLogger(PipelineExecutor.class);
    private static final Path MCR_CACHE_PARENT_DIR = Paths.get("/tmp");
    public static final String MCR_CACHE_DIR_PREFIX = "mcr_cache_";

    /** Map from pipeline instance ID to event labels. */
    private static Map<Long, Set<String>> instanceEventLabels = new HashMap<>();

    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private ModelOperations modelOperations = new ModelOperations();
    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();
    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
    private PipelineOperations pipelineOperations = new PipelineOperations();
    private WorkerResourcesOperations workerResourcesOperations = new WorkerResourcesOperations();
    private RuntimeObjectFactory objectFactory = new RuntimeObjectFactory();

    // Fields used for debugging.

    private List<TaskRequest> taskRequests = new ArrayList<>();

    /**
     * Launch a new {@link PipelineInstance} for this {@link Pipeline} with optional startNode
     * and/or endNode, plus an optional {@link String} for the name of a {@link ParameterSet}
     * created by a {@link ZiggyEventHandler}. This allows the event handler to transfer information
     * to the pipeline that it needs at runtime.
     */
    public PipelineInstance launch(Pipeline pipeline, String instanceName, PipelineNode startNode,
        PipelineNode endNode, Set<String> eventLabels) {

        List<PipelineInstanceNode> nodesForLaunch = new ArrayList<>();

        /*
         * Lock the current version of the model registry and associate it with this pipeline
         * instance.
         */
        ModelRegistry modelRegistry = modelOperations().lockCurrentRegistry();
        PipelineInstance instance = objectFactory().newPipelineInstance(instanceName, pipeline,
            modelRegistry);

        // Construct the instance nodes for launch.
        List<PipelineNode> startNodes = startNode != null ? List.of(startNode)
            : pipelineOperations().rootNodes(pipeline);
        nodesForLaunch
        .addAll(objectFactory().newInstanceNodes(pipeline, instance, startNodes, endNode));

        if (eventLabels != null) {
            instanceEventLabels.put(instance.getId(), eventLabels);
        }

        for (PipelineInstanceNode node : nodesForLaunch) {
            launchNode(node);
        }

        // Get the current state of the pipeline instance from the database and return same.
        return pipelineInstanceOperations().pipelineInstance(instance.getId());
    }

    /**
     * The transition logic generates the worker task request messages for the next node in this
     * pipeline. This method only executes if the task in question completed successfully.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void transitionToNextInstanceNode(PipelineInstanceNode instanceNode) {

        try {
            Path instanceNodeMcrCacheDir = mcrCacheDir(instanceNode.getPipelineStepName());
            if (Files.exists(instanceNodeMcrCacheDir)) {
                log.debug("Deleting MCR cache directory tree");
                ZiggyFileUtils.deleteDirectoryTree(instanceNodeMcrCacheDir, false);
            }
            List<PipelineInstanceNode> nodesForLaunch = pipelineInstanceNodeOperations()
                .nextPipelineInstanceNodes(instanceNode.getId());

            for (PipelineInstanceNode node : nodesForLaunch) {
                launchNode(node);
            }
            pipelineInstanceNodeOperations()
            .markInstanceNodeTransitionComplete(instanceNode.getId());
        } catch (RuntimeException e) {
            log.error("Pipeline transition from {} to next node failed",
                instanceNode.getPipelineStepName(), e);
            pipelineInstanceNodeOperations().markInstanceNodeTransitionFailed(instanceNode);
        }
    }

    /**
     * Logs the PipelineInstance state based on the aggregate PipelineInstanceNode task counts.
     * Called after the transition logic runs.
     */
    public void logUpdatedInstanceState(PipelineInstance pipelineInstance) {
        TaskCounts taskCounts = pipelineInstanceOperations().taskCounts(pipelineInstance);

        log.info("pipelineInstance id={}, state={}", pipelineInstance.getId(),
            pipelineInstance.getState());
        log.info("{}", taskCounts);
    }

    /**
     * Restart {@link PipelineTask}s that have errored out. This method performs the following
     * steps:
     * <ol>
     * <li>Sets the {@link PipelineInstanceNode}s represented by the tasks into the not-yet-run
     * state.
     * <li>Checks each task to make sure it's in a condition that permits a resubmit.
     * <li>Sets the each task to the WAITING_TO_RUN step.
     * <li>Sends a task request for each task.
     * <li>Sets the instance states back to already-started state.
     * </ol>
     */
    public void restartFailedTasks(Collection<PipelineTask> tasks, boolean doTransitionOnly,
        RunMode restartMode) {
        log.debug("runTask(long) - start");

        // Map between the instance nodes and the tasks.
        Map<PipelineInstanceNode, List<PipelineTask>> nodesToTasks = new HashMap<>();
        for (PipelineTask task : tasks) {
            PipelineInstanceNode node = pipelineTaskOperations().pipelineInstanceNode(task);
            if (!nodesToTasks.containsKey(node)) {
                nodesToTasks.put(node, new ArrayList<>());
            }
            nodesToTasks.get(node).add(task);
        }

        // Perform restarts by instance node.
        for (Entry<PipelineInstanceNode, List<PipelineTask>> entry : nodesToTasks.entrySet()) {
            restartFailedTasksForNode(entry, doTransitionOnly, restartMode);
        }
    }

    /**
     * Performs task restart for all tasks associated with a particular
     * {@link PipelineInstanceNode}.
     */
    private void restartFailedTasksForNode(
        Map.Entry<PipelineInstanceNode, List<PipelineTask>> entry, boolean doTransitionOnly,
        RunMode restartMode) {
        PipelineInstanceNode node = entry.getKey();
        List<PipelineTask> tasks = entry.getValue();
        PipelineNode pipelineNode = node.getPipelineNode();

        // Update the instance node's worker resources -- the pipeline node's settings might
        // have been changed since the last time the tasks were run.
        WorkerResources compositeResources = workerResourcesOperations()
            .compositeWorkerResources(pipelineNode);
        node.setHeapSizeGigabytes(compositeResources.getHeapSizeGigabytes());
        node.setMaxWorkerCount(compositeResources.getMaxWorkerCount());
        node = pipelineInstanceNodeOperations().merge(node);

        log.info("Restarting {} tasks for instance node {} ({})", tasks.size(), node.getId(),
            node.getPipelineStepName());

        pipelineTaskDisplayDataOperations().taskCounts(node);
        logInstanceNodeCounts(node, "initial");

        // Loop over tasks and prepare for restart, including sending the task request message.
        for (PipelineTask task : tasks) {
            restartFailedTask(task, doTransitionOnly, restartMode);
        }

        // Update and log the instance state.
        logUpdatedInstanceState(pipelineInstanceNodeOperations().pipelineInstance(node));

        logInstanceNodeCounts(node, "final");
    }

    private void logInstanceNodeCounts(PipelineInstanceNode node, String initialOrFinal) {
        TaskCounts instanceNodeCounts = pipelineTaskDisplayDataOperations().taskCounts(node);
        log.info("node={}: {}", initialOrFinal, instanceNodeCounts);
    }

    /**
     * Performs restart activities for a single {@link PipelineTask}.
     */
    private void restartFailedTask(PipelineTask task, boolean doTransitionOnly,
        RunMode restartMode) {

        pipelineTaskDataOperations().prepareTaskForRestart(task);

        // If all the subtasks are complete, then we can't resubmit.
        SubtaskCounts subtaskCounts = pipelineTaskDataOperations().subtaskCounts(task);
        if (restartMode == RunMode.RESUBMIT && subtaskCounts.getCompletedSubtaskCount() == subtaskCounts.getTotalSubtaskCount()) {
            alertService().generateAndBroadcastAlert(
                "Restarts", task, Alert.Severity.WARNING, "Unable to resubmit task with all subtasks complete");
            return;
        }
        removeTaskFromKilledTaskList(task);
        if (restartMode != RunMode.RESUME_CURRENT_STEP) {
            pipelineTaskDataOperations().updateProcessingStep(task, ProcessingStep.WAITING_TO_RUN);
        }

        // Send the task message to the supervisor.
        sendTaskRequestMessage(task, Priority.HIGH, doTransitionOnly, restartMode);
    }

    /** Replace with dummy method in unit testing. */
    public void removeTaskFromKilledTaskList(PipelineTask pipelineTask) {
        ZiggyMessenger.publish(new RemoveTaskFromKilledTasksMessage(pipelineTask));
    }

    /**
     * Generate and send out the tasks for the specified node.
     *
     * @param instanceNode
     * @param queueName
     * @return
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    private void launchNode(PipelineInstanceNode instanceNode) {
        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = unitOfWorkGenerator(
            instanceNode.getPipelineNode());

        if (!unitOfWorkGenerator.isInitialized()) {
            throw new PipelineException(
                "Configuration Error: Unable to launch node because no UnitOfWorkGenerator class is defined");
        }

        log.debug("Generating tasks");
        PipelineInstance instance = pipelineInstanceNodeOperations().pipelineInstance(instanceNode);
        List<UnitOfWork> unitsOfWork = generateUnitsOfWork(unitOfWorkGenerator.newInstance(),
            instanceNode, instance);
        log.info("Generated {} tasks for pipeline node {}", unitsOfWork.size(),
            instanceNode.getPipelineStepName());

        if (unitsOfWork.isEmpty()) {
            AlertService.getInstance()
            .generateAndBroadcastAlert("PI", AlertService.DEFAULT_TASK, Severity.ERROR,
                "No tasks generated for " + instanceNode.getPipelineStepName());
            pipelineInstanceOperations().setInstanceToErrorsStalledState(instance);
            throw new PipelineException("Task generation did not generate any tasks!  UOW class: "
                + unitOfWorkGenerator.getClassName());
        }

        // If we're retrying the transition, it's possible that some or all of the tasks that
        // we need already exist in the database, in which case we should use them.
        List<PipelineTask> existingTasks = pipelineInstanceNodeOperations()
            .pipelineTasks(List.of(instanceNode));
        if (!CollectionUtils.isEmpty(existingTasks)) {
            log.info("Retrieved {} pre-existing tasks for step {}", existingTasks.size(),
                instanceNode.getPipelineStepName());
            List<String> existingUowBriefStates = existingTasks.stream()
                .map(s -> s.getUnitOfWork().briefState())
                .collect(Collectors.toList());
            unitsOfWork = unitsOfWork.stream()
                .filter(s -> !existingUowBriefStates.contains(s.briefState()))
                .collect(Collectors.toList());
            launchTasks(existingTasks);
        }
        launchTasks(instanceNode, instance, unitsOfWork);
    }

    public ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineNode node) {
        return pipelineStepOperations().unitOfWorkGenerator(node.getPipelineStepName());
    }

    /**
     * Create new {@link PipelineTask}s and the corresponding {@link TaskRequest} messages.
     */
    private void launchTasks(PipelineInstanceNode instanceNode, PipelineInstance instance,
        List<UnitOfWork> tasks) {
        List<PipelineTask> pipelineTasks = objectFactory.newPipelineTasks(instanceNode, instance,
            tasks);
        launchTasks(pipelineTasks);
    }

    void launchTasks(List<PipelineTask> pipelineTasks) {
        for (PipelineTask pipelineTask : pipelineTasks) {
            sendTaskRequestMessage(pipelineTask,
                pipelineTaskOperations().pipelineInstance(pipelineTask).getPriority(), false,
                RunMode.STANDARD);
        }
    }

    public void persistTaskResults(PipelineTask task) {
        PipelineInstance.Priority persistPriority = pipelineTaskDataOperations().retrying(task)
            ? Priority.HIGHEST
                : Priority.HIGH;
        sendTaskRequestMessage(task, persistPriority, false, RunMode.STANDARD);
    }

    private void sendTaskRequestMessage(PipelineTask task, Priority priority,
        boolean doTransitionOnly, RunMode runMode) {

        if (!taskRequestEnabled()) {
            return;
        }

        log.debug("Generating worker task message for task {}, step {}", task,
            task.getPipelineStepName());

        TaskRequest taskRequest = new TaskRequest(task.getPipelineInstanceId(),
            pipelineTaskOperations().pipelineInstanceNodeId(task),
            pipelineTaskOperations().pipelineNode(task).getId(), task, priority, doTransitionOnly,
            runMode);

        ZiggyMessenger.publish(taskRequest);
        if (storeTaskRequests()) {
            taskRequests.add(taskRequest);
        }
    }

    public static List<UnitOfWork> generateUnitsOfWork(UnitOfWorkGenerator uowGenerator,
        PipelineInstanceNode pipelineInstanceNode) {
        return generateUnitsOfWork(uowGenerator, pipelineInstanceNode, null);
    }

    /**
     * Generates the set of UOWs using the
     * {@link UnitOfWorkGenerator#generateUnitsOfWork(PipelineInstanceNode)} and method of a given
     * {@link UnitOfWorkGenerator} implementation. The resulting {@link UnitOfWork} instance will
     * also contain a property that specifies the class name of the generator.
     */
    public static List<UnitOfWork> generateUnitsOfWork(UnitOfWorkGenerator uowGenerator,
        PipelineInstanceNode pipelineInstanceNode, PipelineInstance instance) {
        log.debug("Generating UOWs...");
        // Produce the tasks.
        Set<String> eventLabels = instance != null ? instanceEventLabels.get(instance.getId())
            : null;
        List<UnitOfWork> uows = uowGenerator.unitsOfWork(pipelineInstanceNode, eventLabels);

        // Add some metadata parameters to all the instances.
        for (UnitOfWork uow : uows) {
            uow.addParameter(new Parameter(UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
                uowGenerator.getClass().getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        }
        log.debug("Generating UOWs...done");

        // Now that the UOWs have their brief states properly assigned, sort them by brief state
        // and return.
        return uows.stream().sorted().collect(Collectors.toList());
    }

    /** Returns the {@link Path} for the MATLAB Compiler Runtime (MCR) cache. */
    public static Path mcrCacheDir(String pipelineStepName) {
        return mcrCacheDirParent().resolve(MCR_CACHE_DIR_PREFIX + pipelineStepName);
    }

    /** Returns the {@link Path} that holds all MCR cache directories. */
    public static Path mcrCacheDirParent() {
        return MCR_CACHE_PARENT_DIR
            .resolve(ZiggyConfiguration.getInstance().getString(PropertyName.USER_NAME.property()));
    }

    public PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    ModelOperations modelOperations() {
        return modelOperations;
    }

    PipelineNodeOperations pipelineNodeOperations() {
        return pipelineNodeOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    public PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }

    PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }

    WorkerResourcesOperations workerResourcesOperations() {
        return workerResourcesOperations;
    }

    RuntimeObjectFactory objectFactory() {
        return objectFactory;
    }

    AlertService alertService() {
        return AlertService.getInstance();
    }

    /**
     * During testing, use Mockito to return false here to avoid actually trying to publish
     * messages.
     */
    public boolean taskRequestEnabled() {
        return true;
    }

    /**
     * Determines whether to store task requests during execution. In testing, use Mockito to return
     * true when this method is called so that the task requests are stored and can be retrieved for
     * inspection.
     */
    boolean storeTaskRequests() {
        return false;
    }

    /**
     * Returns the stored task requests that are submitted during execution. Used only in testing.
     */
    List<TaskRequest> getTaskRequests() {
        return taskRequests;
    }
}
