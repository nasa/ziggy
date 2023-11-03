package gov.nasa.ziggy.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.PipelineOperations.TaskStateSummary;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Unit tests for {@link PipelineOperations}. Legacy methods are not tested, but all methods added
 * since Ziggy version 0.3.0 are tested.
 * <p>
 * In addition to {@link PipelineOperations}, there are implicit tests of some methods in
 * {@link PipelineInstanceCrud} and {@link TestCounts}.
 *
 * @author PT
 */
public class PipelineOperationsTest {

    private PipelineTask task1, task2, task3, task4;
    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode instanceNode, newInstanceNode;
    private PipelineDefinitionNode definitionNode;
    private PipelineModuleDefinition moduleDefinition;
    private PipelineDefinition pipelineDefinition;
    private PipelineOperations pipelineOperations;
    private PipelineTaskCrud pipelineTaskCrud;
    private PipelineInstanceCrud pipelineInstanceCrud;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {

        pipelineOperations = new PipelineOperations();
        pipelineTaskCrud = new PipelineTaskCrud();
        pipelineInstanceCrud = new PipelineInstanceCrud();

        // Construct a pipeline instance with 2 tasks, both of which are in the same instance node.
        DatabaseTransactionFactory.performTransaction(() -> {
            moduleDefinition = new PipelineModuleDefinition("dummy1");
            new PipelineModuleDefinitionCrud().persist(moduleDefinition);
            definitionNode = new PipelineDefinitionNode("dummy2", "dummy3");
            pipelineDefinition = new PipelineDefinition("dummy4");
            pipelineDefinition.setRootNodes(List.of(definitionNode));
            new PipelineDefinitionCrud().persist(pipelineDefinition);
            pipelineInstance = new PipelineInstance(pipelineDefinition);
            pipelineInstanceCrud.persist(pipelineInstance);
            instanceNode = new PipelineInstanceNode(pipelineInstance, definitionNode,
                moduleDefinition);
            new PipelineInstanceNodeCrud().persist(instanceNode);
            pipelineInstance.setEndNode(instanceNode);
            PipelineInstance mergedInstance = pipelineInstanceCrud.merge(pipelineInstance);
            task1 = new PipelineTask(mergedInstance, instanceNode);
            task2 = new PipelineTask(mergedInstance, instanceNode);
            pipelineTaskCrud.persist(List.of(task1, task2));
            return null;
        });
    }

    /**
     * Tests that the
     * {@link PipelineOperations#setTaskState(PipelineTask, gov.nasa.ziggy.pipeline.definition.PipelineTask.State)}
     * method performs correctly in the case where all tasks run to completion without errors. In
     * particular, the pipeline task execution clocks should start and stop at the correct times,
     * the pipeline instance task execution clock should start and stop at the correct times, and
     * the pipeline instance and pipeline instance node task counts should be correct.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSetTaskStateNoErrors() {

        // Check initial states.
        List<PipelineTask> databaseTasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {

                List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll();
                for (PipelineTask task : tasks) {
                    assertTrue(task.getCurrentExecutionStartTimeMillis() <= 0);
                    assertEquals(PipelineTask.State.INITIALIZED, task.getState());
                }
                PipelineInstance instance = pipelineInstanceCrud.retrieveAll().get(0);
                assertTrue(instance.getCurrentExecutionStartTimeMillis() <= 0);
                assertEquals(PipelineInstance.State.INITIALIZED, instance.getState());

                TaskCounts counts = pipelineOperations.taskCounts(instance);
                assertEquals(2, counts.getTaskCount());
                assertEquals(0, counts.getCompletedTaskCount());
                assertEquals(0, counts.getFailedTaskCount());
                assertEquals(0, counts.getSubmittedTaskCount());

                counts = pipelineOperations.taskCounts(tasks.get(0).getPipelineInstanceNode());
                assertEquals(2, counts.getTaskCount());
                assertEquals(0, counts.getCompletedTaskCount());
                assertEquals(0, counts.getFailedTaskCount());
                assertEquals(0, counts.getSubmittedTaskCount());

                return tasks;
            });

        // Move the tasks one at a time to the SUBMITTED state.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary updatedStates = pipelineOperations.setTaskState(databaseTasks.get(0),
                PipelineTask.State.SUBMITTED);
            assertEquals(PipelineTask.State.SUBMITTED, updatedStates.getTask().getState());
            assertEquals(PipelineInstance.State.PROCESSING, updatedStates.getInstance().getState());
            TaskCounts counts = updatedStates.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(1, counts.getSubmittedTaskCount());
            counts = updatedStates.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(1, counts.getSubmittedTaskCount());
            assertTrue(updatedStates.getTask().getCurrentExecutionStartTimeMillis() > 0);
            assertTrue(updatedStates.getInstance().getCurrentExecutionStartTimeMillis() > 0);

            updatedStates = pipelineOperations.setTaskState(databaseTasks.get(1),
                PipelineTask.State.SUBMITTED);
            assertEquals(PipelineTask.State.SUBMITTED, updatedStates.getTask().getState());
            assertEquals(PipelineInstance.State.PROCESSING, updatedStates.getInstance().getState());
            counts = updatedStates.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            counts = updatedStates.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertTrue(updatedStates.getTask().getCurrentExecutionStartTimeMillis() > 0);
            assertTrue(updatedStates.getInstance().getCurrentExecutionStartTimeMillis() > 0);
            return null;
        });

        // Make sure that the state changes are actually persisted to the database
        List<PipelineTask> submittedTasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll();
                for (PipelineTask task : tasks) {
                    assertEquals(PipelineTask.State.SUBMITTED, task.getState());
                    assertTrue(task.getCurrentExecutionStartTimeMillis() > 0);
                }
                PipelineInstance instance = pipelineInstanceCrud.retrieveAll().get(0);
                assertEquals(PipelineInstance.State.PROCESSING, instance.getState());
                assertTrue(instance.getCurrentExecutionStartTimeMillis() > 0);
                return tasks;
            });

        // Move the tasks to the PROCESSING state.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineOperations.setTaskState(submittedTasks.get(0), PipelineTask.State.PROCESSING);
            TaskStateSummary updatedStates = pipelineOperations.setTaskState(submittedTasks.get(1),
                PipelineTask.State.PROCESSING);
            TaskCounts counts = updatedStates.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            counts = updatedStates.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            return null;
        });

        // Move both tasks to the COMPLETED state.
        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll();
            pipelineOperations.setTaskState(tasks.get(0), PipelineTask.State.COMPLETED);
            TaskStateSummary updatedStates = pipelineOperations.setTaskState(tasks.get(1),
                PipelineTask.State.COMPLETED);
            assertEquals(PipelineInstance.State.COMPLETED, updatedStates.getInstance().getState());
            assertTrue(updatedStates.getInstance().getCurrentExecutionStartTimeMillis() <= 0);
            TaskCounts counts = updatedStates.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            counts = updatedStates.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            return null;
        });

        // Make sure the tasks got their clocks stopped.
        DatabaseTransactionFactory.performTransaction(() -> {
            List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll();
            assertTrue(tasks.get(0).getCurrentExecutionStartTimeMillis() <= 0);
            assertTrue(tasks.get(1).getCurrentExecutionStartTimeMillis() <= 0);
            return null;
        });
    }

    /**
     * Tests that the correct state transitions are performed when there are tasks that error out.
     */
    @Test
    public void testSetTaskStateWithErrors() {

        // Move the tasks to the PROCESSING state
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineOperations.setTaskState(task1, PipelineTask.State.PROCESSING);
            pipelineOperations.setTaskState(task2, PipelineTask.State.PROCESSING);
            return null;
        });

        // Move one task to state ERROR.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary updatedStates = pipelineOperations.setTaskState(task1,
                PipelineTask.State.ERROR);

            // Check that the updated states are all correct.
            assertEquals(PipelineInstance.State.ERRORS_RUNNING,
                updatedStates.getInstance().getState());
            assertEquals(PipelineTask.State.ERROR, updatedStates.getTask().getState());
            TaskCounts counts = updatedStates.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(1, counts.getFailedTaskCount());
            counts = updatedStates.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(1, counts.getFailedTaskCount());

            assertTrue(updatedStates.getInstance().getCurrentExecutionStartTimeMillis() > 0);
            assertTrue(updatedStates.getTask().getCurrentExecutionStartTimeMillis() <= 0);
            return null;
        });

        // Move the other task to state COMPLETED.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary updatedStates = pipelineOperations.setTaskState(task2,
                PipelineTask.State.COMPLETED);

            // Check that the updated states are all correct.
            assertEquals(PipelineInstance.State.ERRORS_STALLED,
                updatedStates.getInstance().getState());
            assertEquals(PipelineTask.State.COMPLETED, updatedStates.getTask().getState());
            TaskCounts counts = updatedStates.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(1, counts.getCompletedTaskCount());
            assertEquals(1, counts.getFailedTaskCount());
            counts = updatedStates.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(1, counts.getCompletedTaskCount());
            assertEquals(1, counts.getFailedTaskCount());

            assertTrue(updatedStates.getInstance().getCurrentExecutionStartTimeMillis() <= 0);
            assertTrue(updatedStates.getTask().getCurrentExecutionStartTimeMillis() <= 0);
            return null;
        });
    }

    /**
     * Tests that the correct state transitions occur when there are multiple nodes in the pipeline.
     */
    @Test
    public void testMultipleInstanceNodesNoErrors() {

        // Create a new instance node with 2 tasks in it and attach it to the instance.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition newModuleDefinition = new PipelineModuleDefinition("dummy6");
            new PipelineModuleDefinitionCrud().persist(newModuleDefinition);
            PipelineDefinitionNode newDefinitionNode = new PipelineDefinitionNode("dummy7",
                "dummy7");
            definitionNode.setNextNodes(List.of(newDefinitionNode));
            pipelineDefinition.setRootNodes(List.of(definitionNode));
            new PipelineDefinitionNodeCrud().persist(newDefinitionNode);
            new PipelineDefinitionNodeCrud().merge(definitionNode);
            new PipelineDefinitionCrud().merge(pipelineDefinition);
            newInstanceNode = new PipelineInstanceNode(pipelineInstance, newDefinitionNode,
                newModuleDefinition);
            new PipelineInstanceNodeCrud().persist(newInstanceNode);
            pipelineInstance.setEndNode(newInstanceNode);
            pipelineInstanceCrud.merge(pipelineInstance);
            return null;
        });

        // Move the first 2 tasks to completed and check that all the correct states are
        // generated. Then create 2 new pipeline tasks in the second instance node.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineOperations.setTaskState(task1, PipelineTask.State.COMPLETED);
            TaskStateSummary states = pipelineOperations.setTaskState(task2,
                PipelineTask.State.COMPLETED);
            assertEquals(PipelineInstance.State.PROCESSING, states.getInstance().getState());
            TaskCounts counts = states.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            counts = states.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() > 0);

            task3 = new PipelineTask(states.getInstance(), newInstanceNode);
            task4 = new PipelineTask(states.getInstance(), newInstanceNode);
            pipelineTaskCrud.persist(List.of(task3, task4));

            counts = pipelineOperations.taskCounts(states.getInstance());
            assertEquals(4, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());

            counts = pipelineOperations.taskCounts(newInstanceNode);
            assertEquals(2, counts.getTaskCount());
            assertEquals(0, counts.getSubmittedTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());

            return null;
        });

        // Move the new tasks to state PROCESSING.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineOperations.setTaskState(task3, PipelineTask.State.PROCESSING);
            TaskStateSummary states = pipelineOperations.setTaskState(task4,
                PipelineTask.State.PROCESSING);
            assertEquals(PipelineInstance.State.PROCESSING, states.getInstance().getState());
            TaskCounts counts = states.getInstanceCounts();
            assertEquals(4, counts.getTaskCount());
            assertEquals(4, counts.getSubmittedTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            counts = states.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() > 0);

            return null;
        });

        // Move the tasks to COMPLETED.
        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineOperations.setTaskState(task3, PipelineTask.State.COMPLETED);
            TaskStateSummary states = pipelineOperations.setTaskState(task4,
                PipelineTask.State.COMPLETED);
            assertEquals(PipelineInstance.State.COMPLETED, states.getInstance().getState());
            TaskCounts counts = states.getInstanceCounts();
            assertEquals(4, counts.getTaskCount());
            assertEquals(4, counts.getSubmittedTaskCount());
            assertEquals(4, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            counts = states.getInstanceNodeCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(2, counts.getCompletedTaskCount());
            assertEquals(0, counts.getFailedTaskCount());
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() <= 0);

            return null;
        });
    }

    @Test
    public void testMultipleInstanceNodesWithErrors() {

        // Create a new instance node with 2 tasks in it and attach it to the instance.
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineModuleDefinition newModuleDefinition = new PipelineModuleDefinition("dummy6");
            new PipelineModuleDefinitionCrud().persist(newModuleDefinition);
            PipelineDefinitionNode newDefinitionNode = new PipelineDefinitionNode("dummy7",
                "dummy7");
            definitionNode.setNextNodes(List.of(newDefinitionNode));
            pipelineDefinition.setRootNodes(List.of(definitionNode));
            new PipelineDefinitionNodeCrud().persist(newDefinitionNode);
            new PipelineDefinitionNodeCrud().merge(definitionNode);
            new PipelineDefinitionCrud().merge(pipelineDefinition);
            newInstanceNode = new PipelineInstanceNode(pipelineInstance, newDefinitionNode,
                newModuleDefinition);
            new PipelineInstanceNodeCrud().persist(newInstanceNode);
            pipelineInstance.setEndNode(newInstanceNode);
            pipelineInstanceCrud.merge(pipelineInstance);
            return null;
        });

        // Move the first task into PROCESSING state.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary states = pipelineOperations.setTaskState(task1,
                PipelineTask.State.PROCESSING);
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() > 0);
            return null;
        });

        // Now move the first task into the ERROR state.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary states = pipelineOperations.setTaskState(task1,
                PipelineTask.State.ERROR);
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() > 0);
            assertEquals(PipelineInstance.State.ERRORS_RUNNING, states.getInstance().getState());
            TaskCounts counts = states.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(1, counts.getSubmittedTaskCount());
            assertEquals(0, counts.getCompletedTaskCount());
            assertEquals(1, counts.getFailedTaskCount());
            return null;
        });

        // Move the second task into PROCESSING.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary states = pipelineOperations.setTaskState(task2,
                PipelineTask.State.PROCESSING);
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() > 0);
            return null;
        });

        // When the second task completes, the instance should go to ERRORS_STALLED even
        // though there are pipeline instance nodes that have not yet been run.
        DatabaseTransactionFactory.performTransaction(() -> {
            TaskStateSummary states = pipelineOperations.setTaskState(task2,
                PipelineTask.State.COMPLETED);
            assertTrue(states.getInstance().getCurrentExecutionStartTimeMillis() <= 0);
            assertEquals(PipelineInstance.State.ERRORS_STALLED, states.getInstance().getState());
            TaskCounts counts = states.getInstanceCounts();
            assertEquals(2, counts.getTaskCount());
            assertEquals(2, counts.getSubmittedTaskCount());
            assertEquals(1, counts.getCompletedTaskCount());
            assertEquals(1, counts.getFailedTaskCount());
            return null;
        });
    }
}
