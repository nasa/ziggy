package gov.nasa.ziggy.supervisor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.module.ModuleFatalProcessingException;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.FireTriggerRequest;

/**
 * Unit test class for PipelineInstanceManager.
 *
 * @author PT
 */
public class PipelineInstanceManagerTest {

    private static final String PIPELINE_NAME = "pipeline";
    private static final String INSTANCE_NAME = "instance";
    private static final String START_NODE_NAME = "startnode";
    private static final String END_NODE_NAME = "endnode";

    private PipelineDefinitionCrud pipelineDefinitionCrud = Mockito
        .mock(PipelineDefinitionCrud.class);
    private PipelineInstanceCrud pipelineInstanceCrud = Mockito.mock(PipelineInstanceCrud.class);
    private PipelineOperations pipelineOperations = Mockito.mock(PipelineOperations.class);
    private PipelineInstanceManager pipelineInstanceManager;
    private PipelineDefinition pipelineDefinition;
    private PipelineDefinitionNode startNode, endNode;
    private PipelineInstance instance0, instance1, instance2, instance3;
    private PipelineTask task0 = new PipelineTask();
    private List<PipelineTask> taskList = new ArrayList<>();

    @Before
    public void setup() {

        DatabaseService mockDatabaseService = Mockito.mock(DatabaseService.class);
        DatabaseService.reset();
        DatabaseService.setInstance(mockDatabaseService);

        // Provide a PipelineInstanceManager spy that returns the mocked instances when called upon
        pipelineInstanceManager = Mockito.spy(PipelineInstanceManager.class);
        Mockito.when(pipelineInstanceManager.pipelineDefinitionCrud())
            .thenReturn(pipelineDefinitionCrud);
        Mockito.when(pipelineInstanceManager.pipelineInstanceCrud())
            .thenReturn(pipelineInstanceCrud);
        Mockito.when(pipelineInstanceManager.pipelineOperations()).thenReturn(pipelineOperations);

        // Mock some other stuff
        pipelineDefinition = Mockito.mock(PipelineDefinition.class);

        startNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(startNode.getModuleName()).thenReturn(START_NODE_NAME);

        endNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(endNode.getModuleName()).thenReturn(END_NODE_NAME);

        Mockito.when(pipelineDefinitionCrud.retrieveLatestVersionForName(PIPELINE_NAME))
            .thenReturn(pipelineDefinition);
        Mockito.when(pipelineDefinition.getNodeByName(START_NODE_NAME)).thenReturn(startNode);
        Mockito.when(pipelineDefinition.getNodeByName(END_NODE_NAME)).thenReturn(endNode);

        // Set up 4 pipeline instances and make sure they return the right things
        instance0 = Mockito.mock(PipelineInstance.class);
        Mockito.when(instance0.getId()).thenReturn(22L);
        Mockito.when(instance0.getState()).thenReturn(PipelineInstance.State.COMPLETED);
        instance1 = Mockito.mock(PipelineInstance.class);
        Mockito.when(instance1.getId()).thenReturn(23L);
        Mockito.when(instance1.getState()).thenReturn(PipelineInstance.State.COMPLETED);
        instance2 = Mockito.mock(PipelineInstance.class);
        Mockito.when(instance2.getId()).thenReturn(24L);
        Mockito.when(instance2.getState()).thenReturn(PipelineInstance.State.COMPLETED);
        instance3 = Mockito.mock(PipelineInstance.class);
        Mockito.when(instance3.getId()).thenReturn(25L);
        Mockito.when(instance3.getState()).thenReturn(PipelineInstance.State.COMPLETED);

        // Set up the PipelineInstanceCrud returns
        Mockito.when(pipelineInstanceCrud.retrieve(22L)).thenReturn(instance0);
        Mockito.when(pipelineInstanceCrud.retrieve(23L)).thenReturn(instance1);
        Mockito.when(pipelineInstanceCrud.retrieve(24L)).thenReturn(instance2);
        Mockito.when(pipelineInstanceCrud.retrieve(25L)).thenReturn(instance3);

        taskList.add(task0);
    }

    @After
    public void teardown() {
        DatabaseService.reset();
    }

    /**
     * Tests the initialize() method, and also incidentally tests the constructor for the
     * WorkerFireTriggerRequest.
     */
    @Test
    public void testInitialize() {

        // Finite number of repeats
        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 50, 100);
        assertEquals(PIPELINE_NAME, wftr.getPipelineName());
        assertEquals(INSTANCE_NAME, wftr.getInstanceName());
        assertEquals(START_NODE_NAME, wftr.getStartNodeName());
        assertEquals(END_NODE_NAME, wftr.getEndNodeName());
        assertEquals(50, wftr.getMaxRepeats());
        assertEquals(100, wftr.getRepeatIntervalSeconds());

        pipelineInstanceManager.initialize(wftr);
        assertEquals(pipelineDefinition, pipelineInstanceManager.getPipeline());
        assertEquals(startNode, pipelineInstanceManager.getStartNode());
        assertEquals(endNode, pipelineInstanceManager.getEndNode());
        assertEquals(50, pipelineInstanceManager.getMaxRepeats());
        assertEquals(100000L, pipelineInstanceManager.getRepeatIntervalMillis());

        // Effectively infinite number of repeats
        wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode, endNode, -1, 100);
        pipelineInstanceManager.initialize(wftr);
        assertEquals(Integer.MAX_VALUE, pipelineInstanceManager.getMaxRepeats());
    }

    /**
     * Tests that a single successful trigger fire does what's expected
     */
    @Test
    public void testFireTriggerOnceAndSucceed() {

        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 1, 100);
        pipelineInstanceManager.initialize(wftr);

        Mockito
            .when(pipelineOperations.fireTrigger(ArgumentMatchers.any(PipelineDefinition.class),
                ArgumentMatchers.anyString(), ArgumentMatchers.any(PipelineDefinitionNode.class),
                ArgumentMatchers.any(PipelineDefinitionNode.class), ArgumentMatchers.isNull()))
            .thenReturn(instance0);
        pipelineInstanceManager.fireTrigger();

        // Check that the resulting single execution of the fireTrigger() in the PipelineOperations
        // mock occurred as planned
        assertEquals(22L, pipelineInstanceManager.getCurrentInstanceId());
        assertEquals(1, pipelineInstanceManager.getRepeats());
        assertEquals(0, pipelineInstanceManager.getStatusChecks());
        Mockito.verify(pipelineOperations, Mockito.times(1))
            .fireTrigger(pipelineDefinition, INSTANCE_NAME, startNode, endNode, null);
    }

    /**
     * Test that the multiple fires of the trigger do the right thing when all instances run to
     * completion.
     */
    @Test
    public void testFireTriggerMultipleTimesAndSucceed() {

        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 4, 0);
        pipelineInstanceManager.initialize(wftr);

        // Set up the returns from the PipelineOperations fireTrigger() calls
        Mockito
            .when(pipelineOperations.fireTrigger(ArgumentMatchers.any(PipelineDefinition.class),
                ArgumentMatchers.anyString(), ArgumentMatchers.any(PipelineDefinitionNode.class),
                ArgumentMatchers.any(PipelineDefinitionNode.class), ArgumentMatchers.isNull()))
            .thenReturn(instance0)
            .thenReturn(instance1)
            .thenReturn(instance2)
            .thenReturn(instance3);

        // set the wait states to something tiny so that we don't need hours to run this
        Mockito.when(pipelineInstanceManager.checkAgainIntervalMillis()).thenReturn(10L);
        Mockito.when(pipelineInstanceManager.getRepeatIntervalMillis()).thenReturn(10L);

        // And -- go!
        pipelineInstanceManager.fireTrigger();

        assertEquals(4, pipelineInstanceManager.getRepeats());
        assertEquals(3, pipelineInstanceManager.getStatusChecks());
        Mockito.verify(pipelineOperations, Mockito.times(1))
            .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-0", startNode, endNode, null);
        Mockito.verify(pipelineOperations, Mockito.times(1))
            .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-1", startNode, endNode, null);
        Mockito.verify(pipelineOperations, Mockito.times(1))
            .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-2", startNode, endNode, null);
        Mockito.verify(pipelineOperations, Mockito.times(1))
            .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-3", startNode, endNode, null);
    }

    /**
     * Tests that repetitions halt when a pipeline instance in the loop is in ERRORS_RUNNING state
     */
    @Test
    public void testFireTriggerMultipleTimesWithErrorsRunning() {

        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 4, 0);
        pipelineInstanceManager.initialize(wftr);

        // Set up the returns from the PipelineOperations fireTrigger() calls
        Mockito
            .when(pipelineOperations.fireTrigger(ArgumentMatchers.any(PipelineDefinition.class),
                ArgumentMatchers.anyString(), ArgumentMatchers.any(PipelineDefinitionNode.class),
                ArgumentMatchers.any(PipelineDefinitionNode.class), ArgumentMatchers.isNull()))
            .thenReturn(instance0)
            .thenReturn(instance1)
            .thenReturn(instance2)
            .thenReturn(instance3);

        // set the wait states to something tiny so that we don't need hours to run this
        Mockito.when(pipelineInstanceManager.checkAgainIntervalMillis()).thenReturn(10L);
        Mockito.when(pipelineInstanceManager.getRepeatIntervalMillis()).thenReturn(10L);

        // Set error return status
        Mockito.when(instance1.getState()).thenReturn(PipelineInstance.State.ERRORS_RUNNING);

        // Set up to detect the exception
        ModuleFatalProcessingException exception = assertThrows(
            ModuleFatalProcessingException.class, () -> {
                // And -- go!
                pipelineInstanceManager.fireTrigger();

                assertEquals(2, pipelineInstanceManager.getRepeats());
                assertEquals(2, pipelineInstanceManager.getStatusChecks());
                Mockito.verify(pipelineOperations, Mockito.times(1))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-0", startNode, endNode,
                        null);
                Mockito.verify(pipelineOperations, Mockito.times(1))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-1", startNode, endNode,
                        null);
                Mockito.verify(pipelineOperations, Mockito.times(0))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-2", startNode, endNode,
                        null);
                Mockito.verify(pipelineOperations, Mockito.times(0))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-3", startNode, endNode,
                        null);
            });
        assertEquals(exception.getMessage(),
            "Unable to start pipeline repeat 2 due to errored status of pipeline repeat 1");
    }

    /**
     * Tests that repetitions halt when a pipeline instance in the loop is in ERRORS_STALLED state
     */
    @Test
    public void testFireTriggerMultipleTimesWithErrorsStalled() {

        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 4, 0);
        pipelineInstanceManager.initialize(wftr);

        // Set up the returns from the PipelineOperations fireTrigger() calls
        Mockito
            .when(pipelineOperations.fireTrigger(ArgumentMatchers.any(PipelineDefinition.class),
                ArgumentMatchers.anyString(), ArgumentMatchers.any(PipelineDefinitionNode.class),
                ArgumentMatchers.any(PipelineDefinitionNode.class), ArgumentMatchers.isNull()))
            .thenReturn(instance0)
            .thenReturn(instance1)
            .thenReturn(instance2)
            .thenReturn(instance3);

        // set the wait states to something tiny so that we don't need hours to run this
        Mockito.when(pipelineInstanceManager.checkAgainIntervalMillis()).thenReturn(10L);
        Mockito.when(pipelineInstanceManager.getRepeatIntervalMillis()).thenReturn(10L);

        // Set error return status
        Mockito.when(instance1.getState()).thenReturn(PipelineInstance.State.ERRORS_STALLED);

        // Set up to detect the exception
        ModuleFatalProcessingException exception = assertThrows(
            ModuleFatalProcessingException.class, () -> {

                // And -- go!
                pipelineInstanceManager.fireTrigger();

                assertEquals(2, pipelineInstanceManager.getRepeats());
                assertEquals(2, pipelineInstanceManager.getStatusChecks());
                Mockito.verify(pipelineOperations, Mockito.times(1))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-0", startNode, endNode,
                        null);
                Mockito.verify(pipelineOperations, Mockito.times(1))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-1", startNode, endNode,
                        null);
                Mockito.verify(pipelineOperations, Mockito.times(0))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-2", startNode, endNode,
                        null);
                Mockito.verify(pipelineOperations, Mockito.times(0))
                    .fireTrigger(pipelineDefinition, INSTANCE_NAME + ":-3", startNode, endNode,
                        null);
            });
        assertEquals("Unable to start pipeline repeat 2 due to errored status of pipeline repeat 1",
            exception.getMessage());
    }

    /**
     * Tests that the intervals used for log message delivery are as expected.
     */
    @Test
    public void testIntervals() {

        // For a long requested interval, the check again and log message intervals should be 15
        // minutes
        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 4, 10000);
        pipelineInstanceManager.initialize(wftr);
        assertEquals(15 * 60 * 1000, pipelineInstanceManager.checkAgainIntervalMillis());
        assertEquals(15 * 60 * 1000, pipelineInstanceManager.keepAliveLogMsgIntervalMillis());

        // For a short requested interval, the check again and log message intervals should be
        // equal to the requested interval
        wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode, endNode, 4, 60);
        pipelineInstanceManager.initialize(wftr);
        assertEquals(60 * 1000, pipelineInstanceManager.checkAgainIntervalMillis());
        assertEquals(60 * 1000, pipelineInstanceManager.keepAliveLogMsgIntervalMillis());

        // For a zero requested interval the check again and log message intervals should be
        // equal to 1 second
        wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode, endNode, 4, 0);
        pipelineInstanceManager.initialize(wftr);
        assertEquals(1000, pipelineInstanceManager.checkAgainIntervalMillis());
        assertEquals(1000, pipelineInstanceManager.keepAliveLogMsgIntervalMillis());
    }

    /**
     * Tests that the requested wait for the 2nd pipeline run, and the requested wait for
     * completeion of a not-yet-finished pipeline run, work as expected
     */
    @Test
    public void testWaiting() {
        FireTriggerRequest wftr = new FireTriggerRequest(PIPELINE_NAME, INSTANCE_NAME, startNode,
            endNode, 2, 0);
        pipelineInstanceManager.initialize(wftr);

        // We want to have a 75 millisecond interval between runs, plus a 10 millisecond interval
        // between rechecks; the interval for keep-alive messages should be 50 milliseconds
        Mockito.when(pipelineInstanceManager.checkAgainIntervalMillis()).thenReturn(10L);
        Mockito.when(pipelineInstanceManager.repeatIntervalMillis()).thenReturn(75L);
        Mockito.when(pipelineInstanceManager.keepAliveLogMsgIntervalMillis()).thenReturn(50L);

        // We want the first instance to initially still be running and need a recheck
        Mockito.when(instance0.getState())
            .thenReturn(PipelineInstance.State.PROCESSING)
            .thenReturn(PipelineInstance.State.COMPLETED);

        // Set up the returns from the PipelineOperations fireTrigger() calls
        Mockito
            .when(pipelineOperations.fireTrigger(ArgumentMatchers.any(PipelineDefinition.class),
                ArgumentMatchers.anyString(), ArgumentMatchers.any(PipelineDefinitionNode.class),
                ArgumentMatchers.any(PipelineDefinitionNode.class), ArgumentMatchers.isNull()))
            .thenReturn(instance0)
            .thenReturn(instance1);

        long startTimeMillis = System.currentTimeMillis();
        pipelineInstanceManager.fireTrigger();
        long completeTimeMillis = System.currentTimeMillis();
        long dt = completeTimeMillis - startTimeMillis;

        // Make the interval long enough to tolerate some overhead time
        assertTrue(dt > 85 && dt <= 105);
    }
}
