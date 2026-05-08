package gov.nasa.ziggy.services.config;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;

public class DirectoryPropertiesTest {

    // Fields and methods are sorted alphabetically.

    @Rule
    public ZiggyPropertyRule databaseBinDir = new ZiggyPropertyRule(
        PropertyName.DATABASE_BIN_DIR.property(), "pipeline-results/db/bin");

    @Rule
    public ZiggyPropertyRule databaseConfFile = new ZiggyPropertyRule(
        PropertyName.DATABASE_CONF_FILE.property(), "pipeline-results/db/db.conf");

    @Rule
    public ZiggyPropertyRule databaseDir = new ZiggyPropertyRule(
        PropertyName.DATABASE_DIR.property(), "pipeline-results/db");

    @Rule
    public ZiggyPropertyRule databaseSchemaDir = new ZiggyPropertyRule(
        PropertyName.DATABASE_SCHEMA_DIR.property(), "schema");

    @Rule
    public ZiggyPropertyRule dataReceiptDir = new ZiggyPropertyRule(
        PropertyName.DATA_RECEIPT_DIR.property(), "pipeline-results/data-receipt");

    @Rule
    public ZiggyPropertyRule datastoreRootDir = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR.property(), "pipeline-results/datastore");

    @Rule
    public ZiggyPropertyRule pipelineDefsDir = new ZiggyPropertyRule(
        PropertyName.PIPELINE_DEFS_DIR.property(), "etc/ziggy.d");

    @Rule
    public ZiggyPropertyRule pipelineHomeDir = new ZiggyPropertyRule(
        PropertyName.PIPELINE_HOME_DIR.property(), "pipeline");

    @Rule
    public ZiggyPropertyRule resultsDir = new ZiggyPropertyRule(PropertyName.RESULTS_DIR.property(),
        "pipeline-results");

    @Rule
    public ZiggyPropertyRule workingDir = new ZiggyPropertyRule(PropertyName.WORKING_DIR.property(),
        "/path/to/user");

    @Rule
    public ZiggyPropertyRule ziggyBuildDir = new ZiggyPropertyRule(
        PropertyName.ZIGGY_BUILD_DIR.property(), "build");

    @Rule
    public ZiggyPropertyRule ziggyHomeDir = new ZiggyPropertyRule(
        PropertyName.ZIGGY_HOME_DIR.property(), "ziggy");

    @Test
    public void testAlgorithmLogsDir() {
        assertEquals("pipeline-results/log/algorithms",
            DirectoryProperties.algorithmLogsDir().toString());
    }

    @Test
    public void testCliLogDir() {
        assertEquals("pipeline-results/log/cli", DirectoryProperties.cliLogDir().toString());
    }

    @Test
    public void testDatabaseBinDir() {
        assertEquals("pipeline-results/db/bin", DirectoryProperties.databaseBinDir().toString());
    }

    @Test
    public void testDatabaseConfFile() {
        assertEquals("pipeline-results/db/db.conf",
            DirectoryProperties.databaseConfFile().toString());
    }

    @Test
    public void testDatabaseDir() {
        assertEquals("pipeline-results/db", DirectoryProperties.databaseDir().toString());
    }

    @Test
    public void testDatabaseLogDir() {
        assertEquals("pipeline-results/log/db", DirectoryProperties.databaseLogDir().toString());
    }

    @Test
    public void testDatabaseSchemaDir() {
        assertEquals("schema", DirectoryProperties.databaseSchemaDir().toString());
    }

    @Test
    public void testDataReceiptDir() {
        assertEquals("pipeline-results/data-receipt",
            DirectoryProperties.dataReceiptDir().toString());
    }

    @Test
    public void testDatastoreRootDir() {
        assertEquals("pipeline-results/datastore",
            DirectoryProperties.datastoreRootDir().toString());
    }

    @Test
    public void testLogDir() {
        assertEquals("pipeline-results/log", DirectoryProperties.logDir().toString());
    }

    @Test
    public void testManifestsDir() {
        assertEquals("pipeline-results/manifests", DirectoryProperties.manifestsDir().toString());
    }

    @Test
    public void testPbsLogDir() {
        assertEquals("pipeline-results/log/pbs", DirectoryProperties.pbsLogDir().toString());
    }

    @Test
    public void testPipelineBinDir() {
        assertEquals("pipeline/bin", DirectoryProperties.pipelineBinDir().toString());
    }

    @Test
    public void testPipelineDefinitionDir() {
        assertEquals("etc/ziggy.d", DirectoryProperties.pipelineDefinitionDir().toString());
    }

    @Test
    public void testPipelineHomeDir() {
        assertEquals("pipeline", DirectoryProperties.pipelineHomeDir().toString());
    }

    @Test
    public void testPipelineResultsDir() {
        assertEquals("pipeline-results", DirectoryProperties.pipelineResultsDir().toString());
    }

    @Test
    public void testPythonEnvDir() {
        assertEquals("pipeline/env", DirectoryProperties.pythonEnvDir().toString());
    }

    @Test
    public void testReportsDir() {
        assertEquals("pipeline-results/reports", DirectoryProperties.reportsDir().toString());
    }

    @Test
    public void testRunDir() {
        assertEquals("pipeline-results/run", DirectoryProperties.runDir().toString());
    }

    @Test
    public void testSupervisorLogDir() {
        assertEquals("pipeline-results/log/supervisor",
            DirectoryProperties.supervisorLogDir().toString());
    }

    @Test
    public void testTaskDataDir() {
        assertEquals("pipeline-results/task-data", DirectoryProperties.taskDataDir().toString());
    }

    @Test
    public void testTaskLogDir() {
        assertEquals("pipeline-results/log/ziggy", DirectoryProperties.taskLogDir().toString());
    }

    @Test
    public void testWorkingDir() {
        assertEquals("/path/to/user", DirectoryProperties.workingDir().toString());
    }

    @Test
    public void testZiggyBinDir() {
        assertEquals("ziggy/bin", DirectoryProperties.ziggyBinDir().toString());
    }

    @Test
    public void testZiggyCodeBuildDir() {
        assertEquals("build", DirectoryProperties.ziggyCodeBuildDir().toString());
    }

    @Test
    public void testZiggyDefinitionDir() {
        assertEquals("ziggy/etc/ziggy.d", DirectoryProperties.ziggyDefinitionDir().toString());
    }

    @Test
    public void testZiggyEtcDir() {
        assertEquals("ziggy/etc", DirectoryProperties.ziggyEtcDir().toString());
    }

    @Test
    public void testZiggyHomeDir() {
        assertEquals("ziggy", DirectoryProperties.ziggyHomeDir().toString());
    }

    @Test
    public void testZiggyLibDir() {
        assertEquals("ziggy/lib", DirectoryProperties.ziggyLibDir().toString());
    }

    @Test
    public void testZiggyLogoDir() {
        assertEquals("ziggy/resources/main/images", DirectoryProperties.ziggyLogoDir().toString());
    }

    @Test
    public void testZiggySchemaBuildDir() {
        assertEquals("build/schema", DirectoryProperties.ziggySchemaBuildDir().toString());
    }

    @Test
    public void testZiggySchemaDir() {
        assertEquals("ziggy/schema", DirectoryProperties.ziggySchemaDir().toString());
    }
}
