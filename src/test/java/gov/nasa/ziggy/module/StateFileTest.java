
package gov.nasa.ziggy.module;

import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.MockitoAnnotations;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.StateFile.State;
import gov.nasa.ziggy.module.remote.PbsParameters;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Implements unit tests for {@link StateFile}.
 */
public class StateFileTest {

    private static final String QUEUE_NAME = "42424242";
    private static final String REMOTE_GROUP = "424242";
    private static final RemoteNodeDescriptor ARCHITECTURE = RemoteNodeDescriptor.SANDY_BRIDGE;
    private static final int PFE_ARRIVAL_TIME_MILLIS = 45;
    private static final int PBS_SUBMIT_TIME_MILLIS = 46;
    private static final int MIN_CORES_PER_NODE = 3;
    private static final int MIN_GIGS_PER_NODE = 2;
    private static final int ACTIVE_CORES_PER_NODE = 1;
    private static final int REQUESTED_NODE_COUNT = 2;
    private static final double GIGS_PER_SUBTASK = 1.5;
    public static final String REQUESTED_WALL_TIME = "4:30:00";

    private static File workingDirectory;

    private static final String[] NAMES = { StateFile.PREFIX + "1.2.foo.INITIALIZED_1-0-0",
        StateFile.PREFIX + "1.2.foo.INITIALIZED_1-0-1",
        StateFile.PREFIX + "1.2.foo.INITIALIZED_1-1-0",
        StateFile.PREFIX + "1.2.foo.INITIALIZED_2-0-0", StateFile.PREFIX + "1.2.foo.QUEUED_1-0-0",
        StateFile.PREFIX + "1.2.foo.INITIALIZED_1-0-0",
        StateFile.PREFIX + "1.3.foo.INITIALIZED_1-0-0",
        StateFile.PREFIX + "2.2.foo.INITIALIZED_1-0-0",
        StateFile.PREFIX + "1.2.foo.INITIALIZED_1-0-0", };

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(resultsDirPropertyRule);

    @Before
    public void setUp() throws IOException {
        Files.createDirectories(DirectoryProperties.stateFilesDir());
        workingDirectory = DirectoryProperties.stateFilesDir().toFile();
    }

    @Test
    public void testConstructors() {
        StateFile stateFile = createStateFile();
        testStateFileProperties(new StateFile(stateFile));

        MockitoAnnotations.openMocks(this);
        PipelineTask pipelineTask = createPipelineTask();
        PbsParameters pbsParameters = createPbsParameters();

        stateFile = StateFile.generateStateFile(pipelineTask, pbsParameters, 0);
        stateFile.setPbsSubmitTimeMillis(PBS_SUBMIT_TIME_MILLIS);
        stateFile.setPfeArrivalTimeMillis(PFE_ARRIVAL_TIME_MILLIS);
        stateFile.setRemoteGroup(REMOTE_GROUP);
        testStateFileProperties(stateFile);
    }

    private PipelineTask createPipelineTask() {
        PipelineModuleDefinition moduleDefinition = new PipelineModuleDefinition("any");
        PipelineDefinition pipelineDefinition = new PipelineDefinition("any");

        PipelineInstance instance = new PipelineInstance();
        instance.setId(42L);

        PipelineTask task = new PipelineTask(instance, new PipelineInstanceNode(instance,
            new PipelineDefinitionNode(moduleDefinition.getName(), pipelineDefinition.getName()),
            moduleDefinition));
        task.setId(43L);

        return task;
    }

    private PbsParameters createPbsParameters() {
        PbsParameters pbsParameters = new PbsParameters();

        pbsParameters.setActiveCoresPerNode(ACTIVE_CORES_PER_NODE);
        pbsParameters.setArchitecture(ARCHITECTURE);
        pbsParameters.setMinCoresPerNode(MIN_CORES_PER_NODE);
        pbsParameters.setMinGigsPerNode(MIN_GIGS_PER_NODE);
        pbsParameters.setQueueName(QUEUE_NAME);
        pbsParameters.setRemoteGroup(REMOTE_GROUP);
        pbsParameters.setRequestedNodeCount(REQUESTED_NODE_COUNT);
        pbsParameters.setRequestedWallTime(REQUESTED_WALL_TIME);
        pbsParameters.setGigsPerSubtask(GIGS_PER_SUBTASK);

        return pbsParameters;
    }

    @Test
    public void testEqualsAndHash() {
        for (String name1 : NAMES) {
            StateFile a = new StateFile(name1);
            assertFalse(a.equals(null));

            for (String name2 : NAMES) {
                StateFile b = new StateFile(name2);
                if (!name1.equals(name2)) {
                    assertFalse(a.equals(b));
                    assertFalse(b.equals(a));
                } else {
                    assertEquals(a, b);
                    assertEquals(b, a);
                    assertEquals(a.hashCode(), b.hashCode());

                    b = new StateFile(a.getModuleName(), a.getPipelineInstanceId(),
                        a.getPipelineTaskId());
                    b.setState(a.getState());
                    b.setNumTotal(a.getNumTotal());
                    b.setNumComplete(a.getNumComplete());
                    b.setNumFailed(a.getNumFailed());
                    assertEquals(a, b);
                    assertEquals(b, a);
                }
            }
        }
    }

    @Test
    public void testTaskDirName() {
        StateFile a = new StateFile(StateFile.PREFIX + "1.2.hello.QUEUED_3-1-2");
        assertEquals("1-2-hello", a.taskDirName());
    }

    @Test
    public void testName() {
        StateFile stateFile = new StateFile(NAMES[0]);
        assertEquals(StateFile.PREFIX + "1.2.foo.INITIALIZED_1-0-0", stateFile.name());
        assertEquals(StateFile.PREFIX + "1.2.foo", stateFile.invariantPart());
    }

    @Test
    public void testState() {
        StateFile stateFile = new StateFile(NAMES[0]);
        stateFile.setState(State.CLOSED);
        assertEquals(true, stateFile.isDone());
        assertEquals(false, stateFile.isRunning());
        assertEquals(false, stateFile.isQueued());
        stateFile.setState(State.COMPLETE);
        assertEquals(true, stateFile.isDone());
        assertEquals(false, stateFile.isRunning());
        assertEquals(false, stateFile.isQueued());
        stateFile.setState(State.INITIALIZED);
        assertEquals(false, stateFile.isDone());
        assertEquals(false, stateFile.isRunning());
        assertEquals(false, stateFile.isQueued());
        stateFile.setState(State.PROCESSING);
        assertEquals(false, stateFile.isDone());
        assertEquals(true, stateFile.isRunning());
        assertEquals(false, stateFile.isQueued());
        stateFile.setState(State.QUEUED);
        assertEquals(false, stateFile.isDone());
        assertEquals(false, stateFile.isRunning());
        assertEquals(true, stateFile.isQueued());
        stateFile.setState(State.SUBMITTED);
        assertEquals(false, stateFile.isDone());
        assertEquals(false, stateFile.isRunning());
        assertEquals(false, stateFile.isQueued());
    }

    @Test
    public void testDefaultPropertyValues() {
        StateFile stateFile = new StateFile(NAMES[0]);
        assertEquals(StateFile.DEFAULT_REMOTE_NODE_ARCHITECTURE,
            stateFile.getRemoteNodeArchitecture());
        assertEquals(StateFile.DEFAULT_WALL_TIME, stateFile.getRequestedWallTime());
        assertEquals(StateFile.INVALID_STRING, stateFile.getRemoteGroup());
        assertEquals(StateFile.INVALID_STRING, stateFile.getQueueName());
        assertEquals(StateFile.INVALID_VALUE, stateFile.getRequestedNodeCount());
        assertEquals(StateFile.INVALID_VALUE, stateFile.getActiveCoresPerNode());
        assertEquals(StateFile.INVALID_VALUE, stateFile.getMinCoresPerNode());
        assertEquals(StateFile.INVALID_VALUE, stateFile.getMinGigsPerNode());

        assertEquals(StateFile.INVALID_VALUE, stateFile.getPbsSubmitTimeMillis());
        assertEquals(StateFile.INVALID_VALUE, stateFile.getPfeArrivalTimeMillis());
        assertEquals(StateFile.INVALID_VALUE, stateFile.getGigsPerSubtask(), 1e-9);
    }

    @Test
    public void testUpdatedPropertyValues() {
        StateFile stateFile = createStateFile();
        testStateFileProperties(stateFile);
    }

    @Test
    public void testFileIO() {
        StateFile stateFile = createStateFile();

        // Test argument error handling.
        try {
            FileUtils.deleteDirectory(workingDirectory);
        } catch (IOException e) {
            fail("Unexpected exception " + e);
        }

        // Nominal write and read.
        workingDirectory.mkdirs();
        try {
            stateFile.persist();
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }

        try {
            StateFile newStateFile = stateFile.newStateFileFromDiskFile();
            checkEqual(stateFile, newStateFile);
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }

        // Update the state
        StateFile newStateFile = new StateFile(stateFile);
        newStateFile.setState(State.SUBMITTED);
        StateFile.updateStateFile(stateFile, newStateFile);
        try {
            stateFile = newStateFile.newStateFileFromDiskFile();
            checkEqual(stateFile, newStateFile);
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

    private void checkEqual(StateFile stateFile, StateFile newStateFile) {
        assertEquals(stateFile, newStateFile);
        assertEquals(stateFile.hashCode(), newStateFile.hashCode());

        // Check the rest of the properties that aren't in the equals method.
        assertEquals(stateFile.getRemoteNodeArchitecture(),
            newStateFile.getRemoteNodeArchitecture());
        assertEquals(stateFile.getRemoteGroup(), newStateFile.getRemoteGroup());
        assertEquals(stateFile.getQueueName(), newStateFile.getQueueName());
        assertEquals(stateFile.getRequestedWallTime(), newStateFile.getRequestedWallTime());
        assertEquals(stateFile.getActiveCoresPerNode(), newStateFile.getActiveCoresPerNode());
        assertEquals(stateFile.getRequestedNodeCount(), newStateFile.getRequestedNodeCount());
        assertEquals(stateFile.getMinCoresPerNode(), newStateFile.getMinCoresPerNode());
        assertEquals(stateFile.getMinGigsPerNode(), newStateFile.getMinGigsPerNode());

        assertEquals(stateFile.getPbsSubmitTimeMillis(), newStateFile.getPbsSubmitTimeMillis());
        assertEquals(stateFile.getPfeArrivalTimeMillis(), newStateFile.getPfeArrivalTimeMillis());
    }

    private StateFile createStateFile() {
        StateFile stateFile = new StateFile(NAMES[0]);
        stateFile.setRemoteNodeArchitecture(ARCHITECTURE.getNodeName());
        stateFile.setRequestedWallTime(REQUESTED_WALL_TIME);
        stateFile.setRemoteGroup(REMOTE_GROUP);
        stateFile.setQueueName(QUEUE_NAME);
        stateFile.setMinCoresPerNode(MIN_CORES_PER_NODE);
        stateFile.setMinGigsPerNode(MIN_GIGS_PER_NODE);
        stateFile.setActiveCoresPerNode(ACTIVE_CORES_PER_NODE);
        stateFile.setRequestedNodeCount(REQUESTED_NODE_COUNT);
        stateFile.setGigsPerSubtask(GIGS_PER_SUBTASK);

        stateFile.setPbsSubmitTimeMillis(PBS_SUBMIT_TIME_MILLIS);
        stateFile.setPfeArrivalTimeMillis(PFE_ARRIVAL_TIME_MILLIS);

        return stateFile;
    }

    private void testStateFileProperties(StateFile stateFile) {
        assertEquals(ARCHITECTURE.getNodeName(), stateFile.getRemoteNodeArchitecture());
        assertEquals(REQUESTED_WALL_TIME, stateFile.getRequestedWallTime());
        assertEquals(REMOTE_GROUP, stateFile.getRemoteGroup());
        assertEquals(QUEUE_NAME, stateFile.getQueueName());
        assertEquals(MIN_CORES_PER_NODE, stateFile.getMinCoresPerNode());
        assertEquals(MIN_GIGS_PER_NODE, stateFile.getMinGigsPerNode());
        assertEquals(ACTIVE_CORES_PER_NODE, stateFile.getActiveCoresPerNode());
        assertEquals(REQUESTED_NODE_COUNT, stateFile.getRequestedNodeCount());
        assertEquals(GIGS_PER_SUBTASK, stateFile.getGigsPerSubtask(), 1e-9);

        assertEquals(PBS_SUBMIT_TIME_MILLIS, stateFile.getPbsSubmitTimeMillis());
        assertEquals(PFE_ARRIVAL_TIME_MILLIS, stateFile.getPfeArrivalTimeMillis());
    }
}
