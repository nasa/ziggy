package gov.nasa.ziggy.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit tests for {@link PipelineExecutor}.
 * <p>
 * Given the centrality of {@link PipelineExecutor} to Ziggy, one would expect it to be unit tested
 * to within an inch of its live. Au contraire! Most of PipelineExecutor is legacy Spiffy
 * functionality, none of which had any unit tests. There's an open Jira issue, ZIGGY-271, which
 * addresses this. In the meantime, the best we can do is probably to write tests against new
 * functionality.
 *
 * @author PT
 */
public class PipelineExecutorTest {

    private PipelineExecutor pipelineExecutor;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = Mockito
        .mock(PipelineModuleDefinitionOperations.class);
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineInstanceNode nextNode;
    private ClassWrapper<UnitOfWorkGenerator> wrapper;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {

        // Set up a PipelineExecutor that can be modified by Mockito.
        pipelineExecutor = mockedExecutor();

        // Set up a set of pipeline definitions that don't include any persisted tasks.
        pipelineOperationsTestUtils.setUpFourModulePipelineWithInstanceNodes();

        // Get the pipeline instance node we're going to transition away from.
        pipelineInstanceNode = pipelineOperationsTestUtils.getPipelineInstanceNodes().get(0);

        // Get the node we're going to transition to.
        nextNode = pipelineInstanceNodeOperations
            .nextPipelineInstanceNodes(pipelineInstanceNode.getId())
            .get(0);

        // Set up the UOW generator.
        wrapper = Mockito.mock(ClassWrapper.class);
        Mockito.when(wrapper.isInitialized()).thenReturn(true);
        Mockito.when(wrapper.newInstance())
            .thenReturn(new TestUnitOfWorkGenerator(List.of("task1", "task2")));
        Mockito
            .when(pipelineModuleDefinitionOperations.unitOfWorkGenerator(nextNode.getModuleName()))
            .thenReturn(wrapper);
    }

    @Test
    public void testSuccessfulTransition() {

        pipelineExecutor.transitionToNextInstanceNode(pipelineInstanceNode);

        // The completed node should be marked accordingly.
        PipelineInstanceNode databaseNode = pipelineInstanceNodeOperations
            .pipelineInstanceNode(pipelineInstanceNode.getId());
        assertTrue(databaseNode.isTransitionComplete());
        assertFalse(databaseNode.isTransitionFailed());

        // There should be tasks in the database for the next node.
        List<PipelineTask> tasks = pipelineInstanceNodeOperations.pipelineTasks(List.of(nextNode));
        List<String> briefStates = tasks.stream()
            .map(s -> s.getUnitOfWork().briefState())
            .collect(Collectors.toList());
        assertTrue(briefStates.containsAll(List.of("task1", "task2")));
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.containsAll(List.of(1L, 2L)));

        // Task requests should have been sent.
        List<TaskRequest> taskRequests = pipelineExecutor.getTaskRequests();
        List<PipelineTask> messageTasks = taskRequests.stream()
            .map(TaskRequest::getPipelineTask)
            .collect(Collectors.toList());
        assertEquals(tasks, messageTasks);
    }

    @Test
    public void testFailedTransition() {

        // Set up the executor such that an exception will occur when it tries to send the
        // task request messages.
        Mockito.doThrow(PipelineException.class)
            .when(pipelineExecutor)
            .launchTasks(ArgumentMatchers.anyList());

        pipelineExecutor.transitionToNextInstanceNode(pipelineInstanceNode);

        // The failure should be recorded.
        PipelineInstanceNode databaseNode = pipelineInstanceNodeOperations
            .pipelineInstanceNode(pipelineInstanceNode.getId());
        assertFalse(databaseNode.isTransitionComplete());
        assertTrue(databaseNode.isTransitionFailed());

        // There should be tasks in the database for the next node.
        List<PipelineTask> tasks = pipelineInstanceNodeOperations.pipelineTasks(List.of(nextNode));
        List<String> briefStates = tasks.stream()
            .map(s -> s.getUnitOfWork().briefState())
            .collect(Collectors.toList());
        assertTrue(briefStates.containsAll(List.of("task1", "task2")));
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.containsAll(List.of(1L, 2L)));

        // No messages should be sent.
        assertTrue(CollectionUtils.isEmpty(pipelineExecutor.getTaskRequests()));
    }

    @Test
    public void testRetryTransition() {

        // Set up the executor such that an exception will occur when it tries to send the
        // task request messages.
        Mockito.doThrow(PipelineException.class)
            .when(pipelineExecutor)
            .launchTasks(ArgumentMatchers.anyList());

        pipelineExecutor.transitionToNextInstanceNode(pipelineInstanceNode);

        // Create a new executor that won't throw the exception.
        pipelineExecutor = mockedExecutor();

        // Set up the UOW generator such that it returns 3 UOWs: one for each of the existing
        // tasks, plus a new one.
        Mockito.when(wrapper.newInstance())
            .thenReturn(new TestUnitOfWorkGenerator(List.of("task1", "task2", "task3")));

        pipelineExecutor.transitionToNextInstanceNode(pipelineInstanceNode);

        // The completed node should be marked accordingly.
        PipelineInstanceNode databaseNode = pipelineInstanceNodeOperations
            .pipelineInstanceNode(pipelineInstanceNode.getId());
        assertTrue(databaseNode.isTransitionComplete());
        assertFalse(databaseNode.isTransitionFailed());

        // There should be 3 tasks, 2 of which are the ones already created in the
        // database.
        List<PipelineTask> tasks = pipelineInstanceNodeOperations.pipelineTasks(List.of(nextNode));
        List<String> briefStates = tasks.stream()
            .map(s -> s.getUnitOfWork().briefState())
            .collect(Collectors.toList());
        assertTrue(briefStates.containsAll(List.of("task1", "task2", "task3")));
        assertEquals(3, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.containsAll(List.of(1L, 2L, 3L)));

        // Task requests should have been sent.
        List<TaskRequest> taskRequests = pipelineExecutor.getTaskRequests();
        List<PipelineTask> messageTasks = taskRequests.stream()
            .map(TaskRequest::getPipelineTask)
            .collect(Collectors.toList());
        assertEquals(tasks, messageTasks);
    }

    private PipelineExecutor mockedExecutor() {
        PipelineExecutor pipelineExecutor = Mockito.spy(PipelineExecutor.class);
        Mockito.doReturn(pipelineModuleDefinitionOperations)
            .when(pipelineExecutor)
            .pipelineModuleDefinitionOperations();
        Mockito.doReturn(true).when(pipelineExecutor).storeTaskRequests();
        return pipelineExecutor;
    }

    private static final class TestUnitOfWorkGenerator implements UnitOfWorkGenerator {

        private List<String> briefStates;

        public TestUnitOfWorkGenerator(List<String> briefStates) {
            this.briefStates = briefStates;
        }

        @Override
        public List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode) {
            List<UnitOfWork> unitsOfWork = new ArrayList<>();
            for (String briefState : briefStates) {
                UnitOfWork uow = new UnitOfWork();
                uow.setBriefState(briefState);
                unitsOfWork.add(uow);
            }
            return unitsOfWork;
        }

        @Override
        public void setBriefState(UnitOfWork uow, PipelineInstanceNode pipelineInstanceNode) {
        }
    }
}
