package gov.nasa.ziggy.models;

import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_HOME_DIR_PROP_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * @author Todd Klaus
 */
public class ModelMetadataOperationsTest {
    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_HOME_DIR_PROP_NAME, DirectoryProperties.ziggyCodeBuildDir().toString());

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
