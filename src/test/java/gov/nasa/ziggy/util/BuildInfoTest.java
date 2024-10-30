package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.BuildInfo.BuildType;

/** Unit tests for {@link BuildInfo} class. */
public class BuildInfoTest {

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
    public ZiggyPropertyRule ziggyHomePropertyRule = new ZiggyPropertyRule(
        PropertyName.ZIGGY_HOME_DIR.property(), directoryRule, "ziggy");
    public ZiggyPropertyRule pipelineHomePropertyRule = new ZiggyPropertyRule(
        PropertyName.PIPELINE_HOME_DIR.property(), directoryRule, "pipeline");

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(ziggyHomePropertyRule)
        .around(pipelineHomePropertyRule);

    @Test
    public void testWriteZiggyBuildFile() throws IOException {
        writeSampleZiggyVersionFile(true);
        Path buildFilePath = directoryRule.directory()
            .resolve("ziggy")
            .resolve("etc")
            .resolve("ziggy-build.properties");
        assertTrue(Files.isRegularFile(buildFilePath));
        List<String> properties = Files.readAllLines(buildFilePath);
        assertTrue(properties.contains("ziggy.version = Alper"));
        assertTrue(properties.contains("ziggy.version.branch = Bethe"));
        assertTrue(properties.contains("ziggy.version.commit = Gamow"));
    }

    @Test
    public void testGetZiggySoftwareVersion() {
        writeSampleZiggyVersionFile(true);
        assertEquals("Alper", new BuildInfo(BuildType.ZIGGY).getSoftwareVersion());
    }

    @Test(expected = NoSuchElementException.class)
    public void testErrorForMissingZiggyVersion() {
        new BuildInfo(BuildType.ZIGGY).getSoftwareVersion();
    }

    @Test
    public void testGetZiggyBranch() {
        writeSampleZiggyVersionFile(true);
        assertEquals("Bethe", new BuildInfo(BuildType.ZIGGY).getBranch());
    }

    @Test(expected = NoSuchElementException.class)
    public void testErrorForMissingZiggyBranch() {
        new BuildInfo(BuildType.ZIGGY).getBranch();
    }

    @Test
    public void testGetZiggyCommit() {
        writeSampleZiggyVersionFile(true);
        assertEquals("Gamow", new BuildInfo(BuildType.ZIGGY).getRevision());
    }

    @Test(expected = NoSuchElementException.class)
    public void testErrorForMissingZiggyCommit() {
        new BuildInfo(BuildType.ZIGGY).getRevision();
    }

    @Test
    public void testRelease() {
        writeSampleZiggyVersionFile(true);
        assertTrue(new BuildInfo(BuildType.ZIGGY).isRelease());
    }

    @Test
    public void testNotRelease() {
        writeSampleZiggyVersionFile(false);
        assertFalse(new BuildInfo(BuildType.ZIGGY).isRelease());
    }

    @Test
    public void testWritePipelineBuildFile() throws IOException {
        writeSamplePipelineVersionFile();
        Path buildFilePath = directoryRule.directory()
            .resolve("pipeline")
            .resolve("etc")
            .resolve("pipeline-build.properties");
        assertTrue(Files.exists(buildFilePath));
        assertTrue(Files.isRegularFile(buildFilePath));
        List<String> properties = Files.readAllLines(buildFilePath);
        assertTrue(properties.contains("pipeline.version = Alper"));
        assertTrue(properties.contains("pipeline.version.branch = Bethe"));
        assertTrue(properties.contains("pipeline.version.commit = Gamow"));
    }

    @Test
    public void testPipelineVersion() {
        BuildInfo pipelineInfo = new BuildInfo(BuildType.PIPELINE);
        assertEquals(BuildInfo.MISSING_PROPERTY_VALUE, pipelineInfo.getSoftwareVersion());
        assertEquals(BuildInfo.MISSING_PROPERTY_VALUE, pipelineInfo.getBranch());
        assertEquals(BuildInfo.MISSING_PROPERTY_VALUE, pipelineInfo.getRevision());
        writeSamplePipelineVersionFile();
        assertEquals("Alper", pipelineInfo.getSoftwareVersion());
        assertEquals("Bethe", pipelineInfo.getBranch());
        assertEquals("Gamow", pipelineInfo.getRevision());
    }

    private void writeSampleZiggyVersionFile(boolean release) {
        String versionString = release ? "Alper" : "Alper-Gamow";
        BuildInfo ziggyVersion = Mockito.spy(new BuildInfo(BuildType.ZIGGY));
        Mockito.doReturn(List.of(versionString))
            .when(ziggyVersion)
            .externalProcessResults(BuildInfo.VERSION_ARGS);
        Mockito.doReturn(List.of("Bethe"))
            .when(ziggyVersion)
            .externalProcessResults(BuildInfo.BRANCH_ARGS);
        Mockito.doReturn(List.of("Gamow"))
            .when(ziggyVersion)
            .externalProcessResults(BuildInfo.COMMIT_ARGS);
        ziggyVersion.writeBuildFile();
    }

    private void writeSamplePipelineVersionFile() {
        BuildInfo pipelineVersion = Mockito.spy(new BuildInfo(BuildType.PIPELINE));
        Mockito.doReturn(List.of("Alper"))
            .when(pipelineVersion)
            .externalProcessResults(BuildInfo.VERSION_ARGS);
        Mockito.doReturn(List.of("Bethe"))
            .when(pipelineVersion)
            .externalProcessResults(BuildInfo.BRANCH_ARGS);
        Mockito.doReturn(List.of("Gamow"))
            .when(pipelineVersion)
            .externalProcessResults(BuildInfo.COMMIT_ARGS);
        pipelineVersion.writeBuildFile();
    }
}
