package gov.nasa.ziggy.models;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.database.TestUtils;

/**
 * @author Todd Klaus
 */
public class ModelMetadataOperationsTest {
    @Before
    public void setUp() throws Exception {
        TestUtils.setUpDatabase();
        String workingDir = System.getProperty("user.dir");
        Path homeDirPath = Paths.get(workingDir, "build");
        System.setProperty(PropertyNames.ZIGGY_HOME_DIR_PROP_NAME, homeDirPath.toString());
    }

    @After
    public void tearDown() {
        TestUtils.tearDownDatabase();
        System.clearProperty(PropertyNames.ZIGGY_HOME_DIR_PROP_NAME);
    }

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
