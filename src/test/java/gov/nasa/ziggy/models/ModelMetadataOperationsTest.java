package gov.nasa.ziggy.models;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import gov.nasa.ziggy.IntegrationTestCategory;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * @author Todd Klaus
 */
@Category(IntegrationTestCategory.class)
public class ModelMetadataOperationsTest {
    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

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
