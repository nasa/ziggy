package gov.nasa.ziggy.models;

import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_HOME_DIR_PROP_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Paths;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * @author Todd Klaus
 */
public class ModelMetadataOperationsTest {
    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_HOME_DIR_PROP_NAME,
        Paths.get(System.getProperty("user.dir"), "build").toString());

    @Test
    public void testReport() {
        ModelRegistryOperations ops = new ModelRegistryOperations();
        String report = ops.report();

        assertNotNull("report");
        assertFalse("report.isEmpty", report.isEmpty());
    }

    @Test
    public void testReportWithInstanceThatHasNoRegistry() {
        ModelRegistryOperations ops = new ModelRegistryOperations();
        PipelineInstance instanceWithNoRegistry = new PipelineInstance();

        String report = ops.report(instanceWithNoRegistry);

        assertNotNull("report");
        assertFalse("report.isEmpty", report.isEmpty());
    }
}
