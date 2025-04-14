package gov.nasa.ziggy.pipeline.step.remote;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionImporter;

public class RemoteEnvironmentOperationsTest {

    private RemoteEnvironment nasEnvironment;
    private RemoteEnvironment bauhausEnvironment;

    @Rule
    public ZiggyDatabaseRule ziggyDatabaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            List.of(TEST_DATA.resolve("pd-hyperion.xml")));
        List<RemoteEnvironment> remoteEnvironments = pipelineDefinitionImporter
            .remoteEnvironmentImportConditioner()
            .remoteEnvironmentsToPersist();
        if (remoteEnvironments.get(0).getName().equals("nas")) {
            nasEnvironment = remoteEnvironments.get(0);
            bauhausEnvironment = remoteEnvironments.get(1);
        } else {
            nasEnvironment = remoteEnvironments.get(1);
            bauhausEnvironment = remoteEnvironments.get(0);
        }
    }

    @Test
    public void testOperationsMethods() {
        RemoteEnvironmentOperations remoteEnvironmentOperations = new RemoteEnvironmentOperations();
        remoteEnvironmentOperations.persist(nasEnvironment);
        RemoteEnvironment retrievedEnvironment = remoteEnvironmentOperations
            .remoteEnvironment("nas");
        assertEquals(2, retrievedEnvironment.getArchitectures().size());
        assertEquals(3, retrievedEnvironment.getQueues().size());

        retrievedEnvironment.getArchitectures().remove(0);
        RemoteEnvironment newEnvironment = remoteEnvironmentOperations.merge(retrievedEnvironment);
        assertEquals(1, newEnvironment.getArchitectures().size());

        remoteEnvironmentOperations.persist(bauhausEnvironment);
        List<String> remoteEnvironmentNames = remoteEnvironmentOperations.remoteEnvironmentNames();
        assertTrue(remoteEnvironmentNames.contains("bauhaus"));
        assertTrue(remoteEnvironmentNames.contains("nas"));
        assertEquals(2, remoteEnvironmentNames.size());
    }
}
