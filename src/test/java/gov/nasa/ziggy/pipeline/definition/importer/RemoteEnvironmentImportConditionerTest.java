package gov.nasa.ziggy.pipeline.definition.importer;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.step.remote.ArchitectureTestUtils;
import gov.nasa.ziggy.pipeline.step.remote.BatchQueueTestUtils;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.util.PipelineException;

/** Unit tests for {@link RemoteEnvironmentImportConditioner} class. */
public class RemoteEnvironmentImportConditionerTest {

    private RemoteEnvironment nasEnvironment;
    @Rule
    public ZiggyDatabaseRule ziggyDatabaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            List.of(TEST_DATA.resolve("pd-hyperion.xml")));
        List<RemoteEnvironment> remoteEnvironments = pipelineDefinitionImporter
            .getRemoteEnvironments();
        if (remoteEnvironments.get(0).getName().equals("nas")) {
            nasEnvironment = remoteEnvironments.get(0);
        } else {
            nasEnvironment = remoteEnvironments.get(1);
        }
        nasEnvironment.populateDatabaseFields();
    }

    @Test(expected = PipelineException.class)
    public void testExceptionOnDuplicateEnvironments() {
        RemoteEnvironmentImportConditioner conditioner = new RemoteEnvironmentImportConditioner(
            List.of(nasEnvironment, nasEnvironment), false);
        conditioner.remoteEnvironmentsToPersist();
    }

    @Test(expected = PipelineException.class)
    public void testExceptionOnDuplicateArchitectures() {
        nasEnvironment.getArchitectures()
            .add(ArchitectureTestUtils.architectureByName().get("bro"));
        RemoteEnvironmentImportConditioner conditioner = new RemoteEnvironmentImportConditioner(
            List.of(nasEnvironment), false);
        conditioner.remoteEnvironmentsToPersist();
    }

    @Test(expected = PipelineException.class)
    public void testExceptionOnDuplicateQueues() {
        nasEnvironment.getQueues().add(BatchQueueTestUtils.batchQueueByName().get("normal"));
        RemoteEnvironmentImportConditioner conditioner = new RemoteEnvironmentImportConditioner(
            List.of(nasEnvironment), false);
        conditioner.remoteEnvironmentsToPersist();
    }
}
