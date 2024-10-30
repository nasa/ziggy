package gov.nasa.ziggy.pipeline.definition.database;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetric.Units;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskCounts.SubtaskCounts;
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

    private List<PipelineModuleDefinition> pipelineModuleDefinitions;
    private List<PipelineDefinition> pipelineDefinitions;
    private List<PipelineDefinitionNode> pipelineDefinitionNodes;
    private List<PipelineInstance> pipelineInstances;
    private List<PipelineInstanceNode> pipelineInstanceNodes;
    private List<PipelineTask> pipelineTasks;
    private List<PipelineTaskData> pipelineTaskDataList;
    private List<PipelineTaskDisplayData> pipelineTaskDisplayData;

    /**
     * Generates and persists a pipeline definition with a single module and two tasks. Its name is
     * module1 and its pipeline definition's name is pipeline1. The pipeline definition has two
     * parameters parameter1 and parameter2 and the module definition has two parameters parameter3
     * and parameter4.
     */
    public void setUpSingleModulePipeline() {
        generateSingleModulePipeline(true);
    }

    /**
     * Generates and optionally persists a pipeline definition with a single module and two tasks.
     * Its name is module1 and its pipeline definition's name is pipeline1. The pipeline definition
     * has two parameters parameter1 and parameter2 and the module definition has two parameters
     * parameter3 and parameter4.
     */
    public void generateSingleModulePipeline(boolean persist) {
        PipelineModuleDefinition moduleDefinition = new PipelineModuleDefinition("module1");
        PipelineDefinitionNode definitionNode = new PipelineDefinitionNode("module1", "pipeline1");
        PipelineDefinition pipelineDefinition = new PipelineDefinition("pipeline1");
        pipelineDefinition.addRootNode(definitionNode);
        pipelineDefinition
            .setParameterSetNames(ZiggyCollectionUtils.mutableSetOf("parameter1", "parameter2"));
        definitionNode
            .setParameterSetNames(ZiggyCollectionUtils.mutableSetOf("parameter3", "parameter4"));
        PipelineInstance pipelineInstance = new PipelineInstance(pipelineDefinition);
        PipelineInstanceNode instanceNode = new PipelineInstanceNode(definitionNode,
            moduleDefinition);

        // Add the one-to-many relationships between the different objects. This requires a
        // fairly complicated set of operations for the case in which we want all the
        // objects persisted in their final state, and those final states reflected in the
        // fields of this object.
        if (persist) {
            pipelineModuleDefinitions = ZiggyCollectionUtils.mutableListOf(performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(moduleDefinition)));
            pipelineDefinitions = ZiggyCollectionUtils.mutableListOf(
                performTransaction(() -> new PipelineDefinitionCrud().merge(pipelineDefinition)));
            pipelineDefinitionNodes = ZiggyCollectionUtils
                .mutableListOf(pipelineDefinitions.get(0).getRootNodes().get(0));
            pipelineInstances = List
                .of(performTransaction(() -> new PipelineInstanceCrud().merge(pipelineInstance)));

            // Persist the instance node and add it to the definition node, instance, and
            // module definitions. Then replace the existing instance fields in each case.
            pipelineInstanceNodes = List
                .of(performTransaction(() -> new PipelineInstanceNodeCrud().merge(instanceNode)));
            pipelineInstances.get(0).addRootNode(pipelineInstanceNodes.get(0));
            pipelineInstances.get(0).setEndNode(pipelineInstanceNodes.get(0));
            pipelineInstances.get(0).addPipelineInstanceNode(pipelineInstanceNodes.get(0));
            pipelineInstances = ZiggyCollectionUtils.mutableListOf(performTransaction(
                () -> new PipelineInstanceCrud().merge(pipelineInstances.get(0))));
            pipelineDefinitionNodes = ZiggyCollectionUtils.mutableListOf(performTransaction(
                () -> new PipelineDefinitionNodeCrud().merge(pipelineDefinitionNodes.get(0))));
            pipelineModuleDefinitions = ZiggyCollectionUtils.mutableListOf(performTransaction(
                () -> new PipelineModuleDefinitionCrud().merge(pipelineModuleDefinitions.get(0))));

            // Persist the pipeline tasks and update the pipeline instance node.
            List<UnitOfWork> unitsOfWork = List.of(new UnitOfWork("brief0"),
                new UnitOfWork("brief1"));
            pipelineTasks = new RuntimeObjectFactory().newPipelineTasks(
                pipelineInstanceNodes.get(0), pipelineInstances.get(0), unitsOfWork);
            pipelineTaskDataList = new TestOperations().createPipelineTaskData(pipelineTasks);
        } else {
            PipelineTask task1 = new PipelineTask(pipelineInstance, instanceNode, null);
            PipelineTask task2 = new PipelineTask(pipelineInstance, instanceNode, null);
            pipelineModuleDefinitions = ZiggyCollectionUtils.mutableListOf(moduleDefinition);
            pipelineDefinitions = ZiggyCollectionUtils.mutableListOf(pipelineDefinition);
            pipelineDefinitionNodes = List.of(definitionNode);
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
        assertEquals(pipelineTask.getModuleName(), pipelineTaskDisplayData.getModuleName());
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
     * Generates and persists a pipeline definition with a four modules and no tasks. The names are
     * module1 through module4 and its pipeline definition's name is pipeline1.
     */
    public void setUpFourModulePipeline() {
        performTransaction(() -> {
            PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
            PipelineDefinitionCrud pipelineDefinitionCrud = new PipelineDefinitionCrud();
            PipelineDefinitionNodeCrud pipelineDefinitionNodeCrud = new PipelineDefinitionNodeCrud();
            PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();

            PipelineModuleDefinition moduleDefinition = pipelineModuleDefinitionCrud
                .merge(new PipelineModuleDefinition("module1"));
            PipelineModuleDefinition moduleDefinition2 = pipelineModuleDefinitionCrud
                .merge(new PipelineModuleDefinition("module2"));
            PipelineModuleDefinition moduleDefinition3 = pipelineModuleDefinitionCrud
                .merge(new PipelineModuleDefinition("module3"));
            PipelineModuleDefinition moduleDefinition4 = pipelineModuleDefinitionCrud
                .merge(new PipelineModuleDefinition("module4"));
            PipelineDefinitionNode definitionNode = pipelineDefinitionNodeCrud
                .merge(new PipelineDefinitionNode("module1", "pipeline1"));
            PipelineDefinitionNode definitionNode2 = pipelineDefinitionNodeCrud
                .merge(new PipelineDefinitionNode("module2", "pipeline1"));
            PipelineDefinitionNode definitionNode3 = pipelineDefinitionNodeCrud
                .merge(new PipelineDefinitionNode("module3", "pipeline1"));
            PipelineDefinitionNode definitionNode4 = pipelineDefinitionNodeCrud
                .merge(new PipelineDefinitionNode("module4", "pipeline1"));

            definitionNode.addNextNode(definitionNode2);
            definitionNode2.addNextNode(definitionNode3);
            definitionNode3.addNextNode(definitionNode4);
            PipelineDefinition pipelineDefinition = new PipelineDefinition("pipeline1");
            pipelineDefinition.addRootNode(definitionNode);
            new PipelineDefinitionCrud().persist(pipelineDefinition);
            PipelineInstance pipelineInstance = pipelineInstanceCrud
                .merge(new PipelineInstance(pipelineDefinition));
            pipelineDefinition = pipelineDefinitionCrud.merge(pipelineDefinition);
            pipelineModuleDefinitions = ZiggyCollectionUtils.mutableListOf(moduleDefinition,
                moduleDefinition2, moduleDefinition3, moduleDefinition4);
            pipelineDefinitionNodes = ZiggyCollectionUtils.mutableListOf(definitionNode,
                definitionNode2, definitionNode3, definitionNode4);
            pipelineDefinitions = ZiggyCollectionUtils.mutableListOf(pipelineDefinition);
            pipelineInstances = ZiggyCollectionUtils.mutableListOf(pipelineInstance);
            pipelineInstanceNodes = new ArrayList<>();
            pipelineTasks = new ArrayList<>();
        });
    }

    public void setUpFourModulePipelineWithInstanceNodes() {
        setUpFourModulePipeline();
        performTransaction(() -> {
            PipelineInstanceNode instanceNode1 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineDefinitionNodes.get(0),
                    pipelineModuleDefinitions.get(0)));
            pipelineInstance().addRootNode(instanceNode1);
            pipelineInstance().addPipelineInstanceNode(instanceNode1);
            PipelineInstanceNode instanceNode2 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineDefinitionNodes.get(1),
                    pipelineModuleDefinitions.get(1)));
            instanceNode1.getNextNodes().add(instanceNode2);
            pipelineInstance().addPipelineInstanceNode(instanceNode2);
            PipelineInstanceNode instanceNode3 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineDefinitionNodes.get(2),
                    pipelineModuleDefinitions.get(2)));
            instanceNode2.getNextNodes().add(instanceNode3);
            pipelineInstance().addPipelineInstanceNode(instanceNode3);
            PipelineInstanceNode instanceNode4 = new PipelineInstanceNodeCrud()
                .merge(new PipelineInstanceNode(pipelineDefinitionNodes.get(3),
                    pipelineModuleDefinitions.get(3)));
            instanceNode3.getNextNodes().add(instanceNode4);
            pipelineInstance().addPipelineInstanceNode(instanceNode4);
            new PipelineInstanceCrud().merge(pipelineInstance());
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode1));
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode2));
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode3));
            pipelineInstanceNodes.add(new PipelineInstanceNodeCrud().merge(instanceNode4));
        });
    }

    public void setUpTwoPipelineDefinitionsTwoInstancesEach() {
        pipelineInstances = new ArrayList<>();
        pipelineDefinitions = new ArrayList<>();
        performTransaction(() -> {
            PipelineDefinition pipelineDefinition1 = new PipelineDefinitionCrud()
                .merge(new PipelineDefinition("module1"));
            PipelineInstance pipelineInstance1 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipelineDefinition1));
            PipelineInstance pipelineInstance2 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipelineDefinition1));
            pipelineDefinition1 = new PipelineDefinitionCrud().merge(pipelineDefinition1);
            pipelineInstances.add(pipelineInstance1);
            pipelineInstances.add(pipelineInstance2);
            pipelineDefinitions.add(pipelineDefinition1);
        });

        performTransaction(() -> {
            PipelineDefinition pipelineDefinition1 = new PipelineDefinitionCrud()
                .merge(new PipelineDefinition("module2"));
            PipelineInstance pipelineInstance1 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipelineDefinition1));
            PipelineInstance pipelineInstance2 = new PipelineInstanceCrud()
                .merge(new PipelineInstance(pipelineDefinition1));
            pipelineDefinition1 = new PipelineDefinitionCrud().merge(pipelineDefinition1);
            pipelineInstances.add(pipelineInstance1);
            pipelineInstances.add(pipelineInstance2);
            pipelineDefinitions.add(pipelineDefinition1);
        });
    }

    /**
     * Creates five modules (module1 .. module5) in a single pipeline (pipeline1). These are not
     * saved to the database, so this call is fast. For testing purposes, here are the values
     * assigned to the pipeline tasks associated with these modules.
     * <table>
     * <tr>
     * <td><b>Module</b></td>
     * <td><b>Task ID</b></td>
     * <td><b>Processing Step</b></td>
     * <td><b>State</b></td>
     * <td><b>Subtasks</b></td>
     * <td><b>Subtasks Completed</b></td>
     * <td><b>Subtasks Failed</b></td>
     * </tr>
     * <tr>
     * <td>module1</td>
     * <td>1</td>
     * <td>INITIALIZING</td>
     * <td>INITIALIZED</td>
     * <td>10</td>
     * <td>9</td>
     * <td>1</td>
     * </tr>
     * <tr>
     * <td>module2</td>
     * <td>2</td>
     * <td>WAITING_TO_RUN</td>
     * <td>SUBMITTED</td>
     * <td>20</td>
     * <td>18</td>
     * <td>2</td>
     * </tr>
     * <tr>
     * <td>module3</td>
     * <td>3</td>
     * <td>EXECUTING</td>
     * <td>PROCESSING</td>
     * <td>30</td>
     * <td>27</td>
     * <td>3</td>
     * </tr>
     * <tr>
     * <td>module4</td>
     * <td>4</td>
     * <td>EXECUTING</td>
     * <td>PROCESSING</td>
     * <td>40</td>
     * <td>36</td>
     * <td>4</td>
     * </tr>
     * <tr>
     * <td>module5</td>
     * <td>5</td>
     * <td>COMPLETE</td>
     * <td>COMPLETED</td>
     * <td>50</td>
     * <td>45</td>
     * <td>5</td>
     * </tr>
     * </table>
     * Note that pipelineTask("module4").isError() is true.
     */
    public void setUpFivePipelineTasks() {
        pipelineTasks = new ArrayList<>();
        pipelineTasks.add(pipelineTask("module1", 1L, 10, ProcessingStep.INITIALIZING));
        pipelineTasks.add(pipelineTask("module2", 2L, 20, ProcessingStep.WAITING_TO_RUN));
        pipelineTasks.add(pipelineTask("module3", 3L, 30, ProcessingStep.EXECUTING));
        pipelineTasks.add(pipelineTask("module4", 4L, 40, ProcessingStep.EXECUTING, true));
        pipelineTasks.add(pipelineTask("module5", 5L, 50, ProcessingStep.COMPLETE));
    }

    private PipelineTask pipelineTask(String moduleName, Long id, int attributeSeed,
        ProcessingStep processingStep) {
        return pipelineTask(moduleName, id, attributeSeed, processingStep, false);
    }

    private PipelineTask pipelineTask(String moduleName, Long id, int attributeSeed,
        ProcessingStep processingStep, boolean error) {
        PipelineInstanceNode pipelineInstanceNode = new PipelineInstanceNode();
        pipelineInstanceNode.setPipelineModuleDefinition(new PipelineModuleDefinition(moduleName));
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
            .add(pipelineTaskDisplayData("module1", 1L, 10, ProcessingStep.INITIALIZING));
        pipelineTaskDisplayData
            .add(pipelineTaskDisplayData("module2", 2L, 20, ProcessingStep.WAITING_TO_RUN));
        pipelineTaskDisplayData
            .add(pipelineTaskDisplayData("module3", 3L, 30, ProcessingStep.EXECUTING));
        pipelineTaskDisplayData
            .add(pipelineTaskDisplayData("module4", 4L, 40, ProcessingStep.EXECUTING, true));
        pipelineTaskDisplayData
            .add(pipelineTaskDisplayData("module5", 5L, 50, ProcessingStep.COMPLETE));
    }

    private PipelineTaskDisplayData pipelineTaskDisplayData(String moduleName, Long id,
        int attributeSeed, ProcessingStep processingStep) {
        return pipelineTaskDisplayData(moduleName, id, attributeSeed, processingStep, false);
    }

    private PipelineTaskDisplayData pipelineTaskDisplayData(String moduleName, Long id,
        int attributeSeed, ProcessingStep processingStep, boolean error) {
        PipelineInstanceNode pipelineInstanceNode = new PipelineInstanceNode();
        pipelineInstanceNode.setPipelineModuleDefinition(new PipelineModuleDefinition(moduleName));
        UnitOfWork unitOfWork = new UnitOfWork(moduleName);
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

    public List<PipelineModuleDefinition> getPipelineModuleDefinitions() {
        return pipelineModuleDefinitions;
    }

    public List<PipelineDefinition> getPipelineDefinitions() {
        return pipelineDefinitions;
    }

    public List<PipelineDefinitionNode> getPipelineDefinitionNodes() {
        return pipelineDefinitionNodes;
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

    public PipelineModuleDefinition pipelineModuleDefinition() {
        checkState(pipelineModuleDefinitions.size() == 1, "PipelineModuleDefinitions size != 1");
        return pipelineModuleDefinitions.get(0);
    }

    public PipelineDefinition pipelineDefinition() {
        checkState(pipelineDefinitions.size() == 1, "PipelineDefinitions size != 1");
        return pipelineDefinitions.get(0);
    }

    public PipelineDefinitionNode pipelineDefinitionNode() {
        checkState(pipelineDefinitionNodes.size() == 1, "PipelineDefinitionNodes size != 1");
        return pipelineDefinitionNodes.get(0);
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
                    createPipelineTaskMetrics(pipelineTask.getModuleName()));
                pipelineTaskData.setRemoteJobs(createRemoteJobs(pipelineTask));
                pipelineTaskData.setZiggySoftwareRevision("ziggy software revision 1");
                pipelineTaskData.setPipelineSoftwareRevision("pipeline software revision 1");
                pipelineTaskData.setWorkerHost("host" + pipelineTask.getId());
                pipelineTaskData.setWorkerThread(pipelineTask.getId().intValue());

                pipelineTaskDataList.add(pipelineTaskDataCrud.merge(pipelineTaskData));
            }

            return pipelineTaskDataList;
        }

        private List<PipelineTaskMetric> createPipelineTaskMetrics(String moduleName) {
            return new ArrayList<>(List.of(new PipelineTaskMetric(moduleName, 42, Units.TIME)));
        }

        private Set<RemoteJob> createRemoteJobs(PipelineTask pipelineTask) {
            return new HashSet<>(Set.of(new RemoteJob(10 * pipelineTask.getId()),
                new RemoteJob(20 * pipelineTask.getId())));
        }
    }
}
