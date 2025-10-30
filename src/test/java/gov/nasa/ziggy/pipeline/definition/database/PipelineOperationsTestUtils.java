package gov.nasa.ziggy.pipeline.definition.database;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Units;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.ZiggyCollectionUtils;

/**
 * Utility methods for testing operations classes related to the definition of pipeline elements.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineOperationsTestUtils extends DatabaseOperations {

    private List<PipelineStep> pipelineSteps;
    private List<Pipeline> pipelines;
    private List<PipelineNode> pipelineNodes;
    private List<PipelineInstance> pipelineInstances;
    private List<PipelineInstanceNode> pipelineInstanceNodes;
    private List<PipelineTask> pipelineTasks;
    private List<PipelineTaskData> pipelineTaskDataList;
    private List<PipelineTaskDisplayData> pipelineTaskDisplayData;

    /**
     * Generates and persists a pipeline with a single node and two tasks. The node's name is step1
     * and the pipeline's name is pipeline1. The pipeline has two parameters parameter1 and
     * parameter2 and the node has two parameters parameter3 and parameter4.
     */
    public void setUpSingleNodePipeline() {
        generateSingleNodePipeline(true);
    }

    /**
     * Generates and optionally persists a pipeline with a single node and two tasks. The node's
     * name is step1 and the pipeline's name is pipeline1. The pipeline has two parameters
     * parameter1 and parameter2 and the node has two parameters parameter3 and parameter4.
     */
    public void generateSingleNodePipeline(boolean persist) {
        PipelineStep pipelineStep = new PipelineStep("step1");
        PipelineNode pipelineNode = new PipelineNode("step1", "pipeline1");
        Pipeline pipeline = new Pipeline("pipeline1");
        pipeline.addRootNode(pipelineNode);
        pipeline
        .setParameterSetNames(ZiggyCollectionUtils.mutableSetOf("parameter1", "parameter2"));
        pipelineNode
        .setParameterSetNames(ZiggyCollectionUtils.mutableSetOf("parameter3", "parameter4"));
        PipelineInstance pipelineInstance = new PipelineInstance(pipeline);
        PipelineInstanceNode instanceNode = new PipelineInstanceNode(pipelineNode, pipelineStep);

        // Add the one-to-many relationships between the different objects. This requires a
        // fairly complicated set of operations for the case in which we want all the
        // objects persisted in their final state, and those final states reflected in the
        // fields of this object.
        if (persist) {
            pipelineSteps = ZiggyCollectionUtils.mutableListOf(
                performTransaction(() -> new PipelineStepCrud().merge(pipelineStep)));
            pipelines = ZiggyCollectionUtils
                .mutableListOf(performTransaction(() -> new PipelineCrud().merge(pipeline)));
            pipelineNodes = ZiggyCollectionUtils
                .mutableListOf(pipelines.get(0).getRootNodes().get(0));
            pipelineInstances = List
                .of(performTransaction(() -> new PipelineInstanceCrud().merge(pipelineInstance)));

            // Persist the instance nodes and associated nodes and steps.
            pipelineInstanceNodes = List
                .of(performTransaction(() -> new PipelineInstanceNodeCrud().merge(instanceNode)));
            pipelineInstances.get(0).addRootNode(pipelineInstanceNodes.get(0));
            pipelineInstances.get(0).setEndNode(pipelineInstanceNodes.get(0));
            pipelineInstances.get(0).addPipelineInstanceNode(pipelineInstanceNodes.get(0));
            pipelineInstances = ZiggyCollectionUtils.mutableListOf(performTransaction(
                () -> new PipelineInstanceCrud().merge(pipelineInstances.get(0))));
            pipelineNodes = ZiggyCollectionUtils.mutableListOf(
                performTransaction(() -> new PipelineNodeCrud().merge(pipelineNodes.get(0))));
            pipelineSteps = ZiggyCollectionUtils.mutableListOf(
                performTransaction(() -> new PipelineStepCrud().merge(pipelineSteps.get(0))));

            // Persist the pipeline tasks and update the pipeline instance node.
            List<UnitOfWork> unitsOfWork = List.of(new UnitOfWork("brief0"),
                new UnitOfWork("brief1"));
            pipelineTasks = new RuntimeObjectFactory().newPipelineTasks(
                pipelineInstanceNodes.get(0), pipelineInstances.get(0), unitsOfWork);
            pipelineTaskDataList = new TestOperations().createPipelineTaskData(pipelineTasks);
        } else {
            PipelineTask task1 = new PipelineTask(pipelineInstance, instanceNode, null);
            PipelineTask task2 = new PipelineTask(pipelineInstance, instanceNode, null);
            pipelineSteps = ZiggyCollectionUtils.mutableListOf(pipelineStep);
            pipelines = ZiggyCollectionUtils.mutableListOf(pipeline);
            pipelineNodes = List.of(pipelineNode);
            pipelineInstances = ZiggyCollectionUtils.mutableListOf(pipelineInstance);
            pipelineInstanceNodes = List.of(instanceNode);
            pipelineTasks = ZiggyCollectionUtils.mutableListOf(task1, task2);
            pipelineInstance.addRootNode(instanceNode);
            pipelineInstance.addPipelineInstanceNode(instanceNode);
            pipelineInstance.setEndNode(instanceNode);
            instanceNode.addPipelineTask(task1);
            instanceNode.addPipelineTask(task2);
        }
    }

    public void testPipelineTaskDisplayData(
        List<PipelineTaskDisplayData> pipelineTaskDisplayDataList) {
        for (PipelineTaskDisplayData pipelineTaskDisplayData : pipelineTaskDisplayDataList) {
            testPipelineTaskDisplayData(pipelineTaskDisplayData);
        }
    }

    public void testPipelineTaskDisplayData(PipelineTaskDisplayData pipelineTaskDisplayData) {
        // Does this object belong to our instance?
        assertEquals((long) pipelineInstance().getId(),
            pipelineTaskDisplayData.getPipelineInstanceId());

        // Does this object belong to one of our tasks?
        PipelineTask pipelineTask = null;
        for (PipelineTask task : pipelineTasks) {
            if (task.getId().longValue() == pipelineTaskDisplayData.getPipelineTaskId()) {
                pipelineTask = task;
                break;
            }
        }
        assertNotNull(pipelineTask);

        // Does this object belong to one of our task data objects?
        PipelineTaskData pipelineTaskData = null;
        for (PipelineTaskData taskData : pipelineTaskDataList) {
            if (taskData.getPipelineTask().equals(pipelineTaskDisplayData.getPipelineTask())) {
                pipelineTaskData = taskData;
                break;
            }
        }
        assertNotNull(pipelineTaskData);

        // Carry on, using our saved pipeline tasks and pipeline task data.
        assertEquals(pipelineTask.getCreated(), pipelineTaskDisplayData.getCreated());
        assertEquals(pipelineTask.getPipelineStepName(),
            pipelineTaskDisplayData.getPipelineStepName());
        assertEquals(pipelineTask.getUnitOfWork().briefState(),
            pipelineTaskDisplayData.getBriefState());

        assertEquals(pipelineTaskData.getZiggySoftwareRevision(),
            pipelineTaskDisplayData.getZiggySoftwareRevision());
        assertEquals(pipelineTaskData.getPipelineSoftwareRevision(),
            pipelineTaskDisplayData.getPipelineSoftwareRevision());
        assertEquals("host" + pipelineTask.getId() + ":" + pipelineTask.getId(),
            pipelineTaskDisplayData.getWorkerName());
        assertEquals(pipelineTaskData.getProcessingStep(),
            pipelineTaskDisplayData.getProcessingStep());
        assertEquals(pipelineTaskData.isError(), pipelineTaskDisplayData.isError());
        assertEquals(pipelineTaskData.getTotalSubtaskCount(),
            pipelineTaskDisplayData.getTotalSubtaskCount());
        assertEquals(pipelineTaskData.getCompletedSubtaskCount(),
            pipelineTaskDisplayData.getCompletedSubtaskCount());
        assertEquals(pipelineTaskData.getFailedSubtaskCount(),
            pipelineTaskDisplayData.getFailedSubtaskCount());
        assertEquals(pipelineTaskData.getFailureCount(), pipelineTaskDisplayData.getFailureCount());
        assertEquals(pipelineTaskData.getExecutionClock().toString(),
            pipelineTaskDisplayData.getExecutionClock().toString());
        assertEquals(pipelineTaskData.getPipelineTaskMetrics(),
            pipelineTaskDisplayData.getPipelineTaskMetrics());
        assertEquals(pipelineTaskData.getRemoteJobs(), pipelineTaskDisplayData.getRemoteJobs());
    }

    /**
     * Generates and persists a pipeline with a four nodes and no tasks. The names are step1 through
     * step4 and the pipeline's name is pipeline1.
     */
    public void setUpFourNodePipeline() {
        performTransaction(() -> {
            PipelineStepCrud pipelineStepCrud = new PipelineStepCrud();
            PipelineCrud pipelineCrud = new PipelineCrud();
            PipelineNodeCrud pipelineNodeCrud = new PipelineNodeCrud();
            PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();

            PipelineStep pipelineStep = pipelineStepCrud.merge(new PipelineStep("step1"));
            PipelineStep pipelineStep2 = pipelineStepCrud.merge(new PipelineStep("step2"));
            PipelineStep pipelineStep3 = pipelineStepCrud.merge(new PipelineStep("step3"));
            PipelineStep pipelineStep4 = pipelineStepCrud.merge(new PipelineStep("step4"));
            PipelineNode pipelineNode = pipelineNodeCrud
                .merge(new PipelineNode("step1", "pipeline1"));
            PipelineNode pipelineNode2 = pipelineNodeCrud
                .merge(new PipelineNode("step2", "pipeline1"));
            PipelineNode pipelineNode3 = pipelineNodeCrud
                .merge(new PipelineNode("step3", "pipeline1"));
            PipelineNode pipelineNode4 = pipelineNodeCrud
                .merge(new PipelineNode("step4", "pipeline1"));

            pipelineNode.addNextNode(pipelineNode2);
            pipelineNode2.addNextNode(pipelineNode3);
            pipelineNode3.addNextNode(pipelineNode4);
            Pipeline pipeline = new Pipeline("pipeline1");
            pipeline.addRootNode(pipelineNode);
            new PipelineCrud().persist(pipeline);
            PipelineInstance pipelineInstance = pipelineInstanceCrud
                .merge(new PipelineInstance(pipeline));
            pipeline = pipelineCrud.merge(pipeline);
            pipelineSteps = ZiggyCollectionUtils.mutableListOf(pipelineStep, pipelineStep2,
                pipelineStep3, pipelineStep4);
            pipelineNodes = ZiggyCollectionUtils.mutableListOf(pipelineNode, pipelineNode2,
                pipelineNode3, pipelineNode4);
            pipelines = ZiggyCollectionUtils.mutableListOf(pipeline);
            pipelineInstances = ZiggyCollectionUtils.mutableListOf(pipelineInstance);
            pipelineInstanceNodes = new ArrayList<>();
            pipelineTasks = new ArrayList<>();
        });
    }

    public void setUpFourNodePipelineWithInstanceNodes() {
        setUpFourNodePipeline();
        performTransaction(() -> {
            PipelineInstanceNode instanceNode1 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineNodes.get(0), pipelineSteps.get(0)));
            pipelineInstance().addRootNode(instanceNode1);
            pipelineInstance().addPipelineInstanceNode(instanceNode1);
            PipelineInstanceNode instanceNode2 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineNodes.get(1), pipelineSteps.get(1)));
            instanceNode1.getNextNodes().add(instanceNode2);
            pipelineInstance().addPipelineInstanceNode(instanceNode2);
            PipelineInstanceNode instanceNode3 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineNodes.get(2), pipelineSteps.get(2)));
            instanceNode2.getNextNodes().add(instanceNode3);
            pipelineInstance().addPipelineInstanceNode(instanceNode3);
            PipelineInstanceNode instanceNode4 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineNodes.get(3), pipelineSteps.get(3)));
            instanceNode3.getNextNodes().add(instanceNode4);
            pipelineInstance().addPipelineInstanceNode(instanceNode4);
            new PipelineInstanceCrud().merge(pipelineInstance());
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode1));
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode2));
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode3));
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode4));
        });
    }

    public void setUpTasksForFourNodePipeline() {
        // Persist the pipeline tasks and update the pipeline instance node.
        List<UnitOfWork> unitsOfWork = List.of(new UnitOfWork("brief0"),
            new UnitOfWork("brief1"));
        pipelineTasks = new RuntimeObjectFactory().newPipelineTasks(
            pipelineInstanceNodes.get(0), pipelineInstances.get(0), unitsOfWork);
        pipelineTaskDataList = new TestOperations().createPipelineTaskData(pipelineTasks);
    }

    public void setUpTwoPipelinesTwoInstancesEach() {
        pipelineInstances = new ArrayList<>();
        pipelines = new ArrayList<>();
        performTransaction(() -> {
            Pipeline pipeline1 = new PipelineCrud().merge(new Pipeline("step1"));
            PipelineInstance pipelineInstance1 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipeline1));
            PipelineInstance pipelineInstance2 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipeline1));
            pipeline1 = new PipelineCrud().merge(pipeline1);
            pipelineInstances.add(pipelineInstance1);
            pipelineInstances.add(pipelineInstance2);
            pipelines.add(pipeline1);
        });

        performTransaction(() -> {
            Pipeline pipeline1 = new PipelineCrud().merge(new Pipeline("step2"));
            PipelineInstance pipelineInstance1 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipeline1));
            PipelineInstance pipelineInstance2 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipeline1));
            pipeline1 = new PipelineCrud().merge(pipeline1);
            pipelineInstances.add(pipelineInstance1);
            pipelineInstances.add(pipelineInstance2);
            pipelines.add(pipeline1);
        });
    }

    /**
     * Creates five nodes (step1 .. step5) in a single pipeline (pipeline1). These are not saved to
     * the database, so this call is fast. For testing purposes, here are the values assigned to the
     * pipeline tasks associated with these nodes.
     * <table>
     * <tr>
     * <td><b>Node</b></td>
     * <td><b>Task ID</b></td>
     * <td><b>Processing Step</b></td>
     * <td><b>State</b></td>
     * <td><b>Subtasks</b></td>
     * <td><b>Subtasks Completed</b></td>
     * <td><b>Subtasks Failed</b></td>
     * </tr>
     * <tr>
     * <td>step1</td>
     * <td>1</td>
     * <td>INITIALIZING</td>
     * <td>INITIALIZED</td>
     * <td>10</td>
     * <td>9</td>
     * <td>1</td>
     * </tr>
     * <tr>
     * <td>step2</td>
     * <td>2</td>
     * <td>WAITING_TO_RUN</td>
     * <td>SUBMITTED</td>
     * <td>20</td>
     * <td>18</td>
     * <td>2</td>
     * </tr>
     * <tr>
     * <td>step3</td>
     * <td>3</td>
     * <td>EXECUTING</td>
     * <td>PROCESSING</td>
     * <td>30</td>
     * <td>27</td>
     * <td>3</td>
     * </tr>
     * <tr>
     * <td>step4</td>
     * <td>4</td>
     * <td>EXECUTING</td>
     * <td>PROCESSING</td>
     * <td>40</td>
     * <td>36</td>
     * <td>4</td>
     * </tr>
     * <tr>
     * <td>step5</td>
     * <td>5</td>
     * <td>COMPLETE</td>
     * <td>COMPLETED</td>
     * <td>50</td>
     * <td>45</td>
     * <td>5</td>
     * </tr>
     * </table>
     * Note that pipelineTask("step4").isError() is true.
     */
    public void setUpFivePipelineTasks() {
        pipelineTasks = new ArrayList<>();
        pipelineTasks.add(pipelineTask("step1", 1L, 10, ProcessingStep.INITIALIZING));
        pipelineTasks.add(pipelineTask("step2", 2L, 20, ProcessingStep.WAITING_TO_RUN));
        pipelineTasks.add(pipelineTask("step3", 3L, 30, ProcessingStep.EXECUTING));
        pipelineTasks.add(pipelineTask("step4", 4L, 40, ProcessingStep.EXECUTING, true));
        pipelineTasks.add(pipelineTask("step5", 5L, 50, ProcessingStep.COMPLETE));
    }

    private PipelineTask pipelineTask(String pipelineStepName, Long id, int attributeSeed,
        ProcessingStep processingStep) {
        return pipelineTask(pipelineStepName, id, attributeSeed, processingStep, false);
    }

    private PipelineTask pipelineTask(String pipelineStepName, Long id, int attributeSeed,
        ProcessingStep processingStep, boolean error) {
        PipelineInstanceNode pipelineInstanceNode = new PipelineInstanceNode();
        pipelineInstanceNode.setPipelineStep(new PipelineStep(pipelineStepName));
        PipelineTask pipelineTask = Mockito.spy(new PipelineTask(null, pipelineInstanceNode, null));
        Mockito.doReturn(id).when(pipelineTask).getId();

        SubtaskCounts subtaskCounts = new SubtaskCounts(attributeSeed,
            attributeSeed - (int) (0.1 * attributeSeed), (int) (0.1 * attributeSeed));
        Mockito.when(Mockito.spy(new PipelineTaskDataOperations()).subtaskCounts(pipelineTask))
        .thenReturn(subtaskCounts);
        // pipelineTask.setProcessingStep(processingStep);
        // pipelineTask.setError(error);
        return pipelineTask;
    }

    public void setUpFivePipelineTaskDisplayData() {
        pipelineTaskDisplayData = new ArrayList<>();
        pipelineTaskDisplayData
        .add(pipelineTaskDisplayData("step1", 1L, 10, ProcessingStep.INITIALIZING));
        pipelineTaskDisplayData
        .add(pipelineTaskDisplayData("step2", 2L, 20, ProcessingStep.WAITING_TO_RUN));
        pipelineTaskDisplayData
        .add(pipelineTaskDisplayData("step3", 3L, 30, ProcessingStep.EXECUTING));
        pipelineTaskDisplayData
        .add(pipelineTaskDisplayData("step4", 4L, 40, ProcessingStep.EXECUTING, true));
        pipelineTaskDisplayData
        .add(pipelineTaskDisplayData("step5", 5L, 50, ProcessingStep.COMPLETE));
    }

    private PipelineTaskDisplayData pipelineTaskDisplayData(String pipelineStepName, Long id,
        int attributeSeed, ProcessingStep processingStep) {
        return pipelineTaskDisplayData(pipelineStepName, id, attributeSeed, processingStep, false);
    }

    private PipelineTaskDisplayData pipelineTaskDisplayData(String pipelineStepName, Long id,
        int attributeSeed, ProcessingStep processingStep, boolean error) {
        PipelineInstanceNode pipelineInstanceNode = new PipelineInstanceNode();
        pipelineInstanceNode.setPipelineStep(new PipelineStep(pipelineStepName));
        UnitOfWork unitOfWork = new UnitOfWork(pipelineStepName);
        PipelineTask pipelineTask = Mockito
            .spy(new PipelineTask(null, pipelineInstanceNode, unitOfWork));
        Mockito.doReturn(id).when(pipelineTask).getId();

        PipelineTaskData pipelineTaskData = new PipelineTaskData(pipelineTask);
        pipelineTaskData.setProcessingStep(processingStep);
        pipelineTaskData.setTotalSubtaskCount(attributeSeed);
        pipelineTaskData.setCompletedSubtaskCount(attributeSeed - (int) (0.1 * attributeSeed));
        pipelineTaskData.setFailedSubtaskCount((int) (0.1 * attributeSeed));
        pipelineTaskData.setError(error);

        return new PipelineTaskDisplayData(pipelineTaskData);
    }

    public List<PipelineStep> getPipelineSteps() {
        return pipelineSteps;
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
    }

    public List<PipelineNode> getPipelineNodes() {
        return pipelineNodes;
    }

    public List<PipelineInstance> getPipelineInstances() {
        return pipelineInstances;
    }

    public List<PipelineInstanceNode> getPipelineInstanceNodes() {
        return pipelineInstanceNodes;
    }

    public List<PipelineTask> getPipelineTasks() {
        return pipelineTasks;
    }

    public List<PipelineTaskDisplayData> getPipelineTaskDisplayData() {
        return pipelineTaskDisplayData;
    }

    // Special-case getters for situations in which there is one and only one element
    // in a list. The "get" prefix is not used because these do not get a field of the
    // instance.

    public PipelineStep pipelineStep() {
        checkState(pipelineSteps.size() == 1, "PipelineSteps size != 1");
        return pipelineSteps.get(0);
    }

    public Pipeline pipeline() {
        checkState(pipelines.size() == 1, "Pipelines size != 1");
        return pipelines.get(0);
    }

    public PipelineNode pipelineNode() {
        checkState(pipelineNodes.size() == 1, "PipelineNodes size != 1");
        return pipelineNodes.get(0);
    }

    public PipelineInstance pipelineInstance() {
        checkState(pipelineInstances.size() == 1, "PipelineInstances size != 1");
        return pipelineInstances.get(0);
    }

    public PipelineInstanceNode pipelineInstanceNode() {
        checkState(pipelineInstanceNodes.size() == 1, "PipelineInstanceNodes size != 1");
        return pipelineInstanceNodes.get(0);
    }

    private static class TestOperations extends DatabaseOperations {
        PipelineTaskDataCrud pipelineTaskDataCrud = new PipelineTaskDataCrud();

        private List<PipelineTaskData> createPipelineTaskData(List<PipelineTask> pipelineTasks) {
            List<PipelineTaskData> pipelineTaskDataList = new ArrayList<>();

            for (PipelineTask pipelineTask : pipelineTasks) {
                PipelineTaskData pipelineTaskData = pipelineTaskDataCrud
                    .retrievePipelineTaskData(pipelineTask);

                pipelineTaskData.setPipelineTaskMetrics(
                    createPipelineTaskMetrics(pipelineTask.getPipelineStepName()));
                pipelineTaskData.setRemoteJobs(createRemoteJobs(pipelineTask));
                pipelineTaskData.setZiggySoftwareRevision("ziggy software revision 1");
                pipelineTaskData.setPipelineSoftwareRevision("pipeline software revision 1");
                pipelineTaskData.setWorkerHost("host" + pipelineTask.getId());
                pipelineTaskData.setWorkerThread(pipelineTask.getId().intValue());

                pipelineTaskDataList.add(pipelineTaskDataCrud.merge(pipelineTaskData));
            }

            return pipelineTaskDataList;
        }

        private List<PipelineTaskMetric> createPipelineTaskMetrics(String pipelineStepName) {
            return new ArrayList<>(
                List.of(new PipelineTaskMetric(pipelineStepName, 42, Units.TIME)));
        }

        private Set<RemoteJob> createRemoteJobs(PipelineTask pipelineTask) {
            return new HashSet<>(Set.of(new RemoteJob(10 * pipelineTask.getId()),
                new RemoteJob(20 * pipelineTask.getId())));
        }
    }
}
