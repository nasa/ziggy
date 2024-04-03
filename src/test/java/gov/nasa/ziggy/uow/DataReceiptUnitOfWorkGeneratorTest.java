package gov.nasa.ziggy.uow;

import static gov.nasa.ziggy.services.config.PropertyName.DATA_RECEIPT_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;

/**
 * Unit tests for the {@link DataReceiptUnitOfWorkGenerator} class.
 *
 * @author PT
 */
public class DataReceiptUnitOfWorkGeneratorTest {

    private Path dataImporterPath;
    private PipelineInstanceNode pipelineInstanceNode;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule dataReceiptDirPropertyRule = new ZiggyPropertyRule(DATA_RECEIPT_DIR,
        directoryRule, "data-import");

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(dataReceiptDirPropertyRule);

    @Before
    public void setUp() throws IOException {

        dataImporterPath = Paths.get(dataReceiptDirPropertyRule.getValue()).toAbsolutePath();
        // Create the data receipt main directory.
        dataImporterPath.toFile().mkdirs();

        // Create some subdirectories.
        Files.createDirectory(dataImporterPath.resolve("subdir-1"));
        Files.createDirectory(dataImporterPath.resolve("subdir-2"));
        Files.createDirectory(dataImporterPath.resolve("bad-name"));

        // Create the pipeline instance
        pipelineInstanceNode = new PipelineInstanceNode();
    }

    @Test
    public void testMultipleUnitsOfWork() throws IOException {
        Files.createFile(dataImporterPath.resolve("subdir-1").resolve("test-manifest.xml"));
        Files.createFile(dataImporterPath.resolve("subdir-2").resolve("test-manifest.xml"));
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .unitsOfWork(pipelineInstanceNode);
        assertEquals(2, unitsOfWork.size());
        Set<String> dirStrings = new HashSet<>();
        dirStrings.add(unitsOfWork.get(0).getParameter("directory").getString());
        dirStrings.add(unitsOfWork.get(1).getParameter("directory").getString());
        assertTrue(dirStrings.contains(dataImporterPath.resolve("subdir-1").toString()));
        assertTrue(dirStrings.contains(dataImporterPath.resolve("subdir-2").toString()));
        Set<String> briefStates = new HashSet<>();
        briefStates.add(unitsOfWork.get(0).briefState());
        briefStates.add(unitsOfWork.get(1).briefState());
        assertTrue(briefStates.contains("subdir-1"));
        assertTrue(briefStates.contains("subdir-2"));
    }

    @Test
    public void testSingleUnitOfWork() throws IOException {
        Files.createFile(dataImporterPath.resolve("test-manifest.xml"));
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .unitsOfWork(pipelineInstanceNode);
        assertEquals(1, unitsOfWork.size());
        assertEquals(dataImporterPath.toString(),
            unitsOfWork.get(0).getParameter("directory").getString());
        assertEquals("data-import", unitsOfWork.get(0).briefState());
    }

    @Test
    public void testSingleUnitOfWorkWithLabel() throws IOException {
        Files.createFile(dataImporterPath.resolve("test-manifest.xml"));
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .unitsOfWork(pipelineInstanceNode, new HashSet<>());
        assertEquals(1, unitsOfWork.size());
        assertEquals(dataImporterPath.toString(),
            unitsOfWork.get(0).getParameter("directory").getString());
        assertEquals("data-import", unitsOfWork.get(0).briefState());
    }

    @Test
    public void testEventHandlerLimitingUows() throws IOException {
        Files.createFile(dataImporterPath.resolve("subdir-1").resolve("test-manifest.xml"));
        Files.createFile(dataImporterPath.resolve("subdir-2").resolve("test-manifest.xml"));
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .unitsOfWork(pipelineInstanceNode, Set.of("subdir-1"));
        assertEquals(1, unitsOfWork.size());
        assertEquals(dataImporterPath.resolve("subdir-1").toString(),
            unitsOfWork.get(0).getParameter("directory").getString());
        assertEquals("subdir-1", unitsOfWork.get(0).briefState());
    }

    @Test
    public void testNoDataReceiptDirectories() {
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .generateUnitsOfWork(pipelineInstanceNode, new HashSet<>());
        assertEquals(0, unitsOfWork.size());
    }
}
