package gov.nasa.ziggy.uow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.events.ZiggyEventLabels;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Unit tests for the {@link DataReceiptUnitOfWorkGenerator} class.
 *
 * @author PT
 */
public class DataReceiptUnitOfWorkGeneratorTest {

    private Path dataImporterPath;
    Map<Class<? extends Parameters>, Parameters> parametersMap;
    TaskConfigurationParameters taskConfig;

    @Before
    public void setUp() throws IOException {

        // Create the data receipt main directory.
        dataImporterPath = Paths.get(System.getProperty("user.dir"), "build", "test",
            "data-import");
        dataImporterPath.toFile().mkdirs();
        System.setProperty(PropertyNames.DATA_RECEIPT_DIR_PROP_NAME, dataImporterPath.toString());

        // Create some subdirectories.
        Files.createDirectory(dataImporterPath.resolve("subdir-1"));
        Files.createDirectory(dataImporterPath.resolve("subdir-2"));
        Files.createDirectory(dataImporterPath.resolve("bad-name"));
        Files.createDirectory(dataImporterPath.resolve(".manifests"));

        // Create the parameters map
        parametersMap = new HashMap<>();
        taskConfig = new TaskConfigurationParameters();
        taskConfig.setTaskDirectoryRegex("(subdir-[0-9]+)");
        parametersMap.put(TaskConfigurationParameters.class, taskConfig);
    }

    @After
    public void tearDown() throws IOException {
        System.clearProperty(PropertyNames.DATA_RECEIPT_DIR_PROP_NAME);
        FileUtils.forceDelete(dataImporterPath.toFile());
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test
    public void testMultipleUnitsOfWork() {
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .generateTasks(parametersMap);
        assertEquals(2, unitsOfWork.size());
        Set<String> dirStrings = new HashSet<>();
        dirStrings.add(unitsOfWork.get(0).getParameter("directory").getString());
        dirStrings.add(unitsOfWork.get(1).getParameter("directory").getString());
        assertTrue(dirStrings.contains("subdir-1"));
        assertTrue(dirStrings.contains("subdir-2"));
    }

    @Test
    public void testSingleUnitOfWork() {
        taskConfig.setTaskDirectoryRegex("");
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .generateTasks(parametersMap);
        assertEquals(1, unitsOfWork.size());
        assertEquals("", unitsOfWork.get(0).getParameter("directory").getString());
    }

    @Test
    public void testEventHandlerLimitingUows() {
        ZiggyEventLabels eventLabels = new ZiggyEventLabels();
        eventLabels.setEventLabels(new String[] { "subdir-1" });
        parametersMap.put(ZiggyEventLabels.class, eventLabels);
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .generateTasks(parametersMap);
        assertEquals(1, unitsOfWork.size());
        assertEquals("subdir-1", unitsOfWork.get(0).getParameter("directory").getString());
    }

    @Test
    public void testEmptyEventLabels() {
        ZiggyEventLabels eventLabels = new ZiggyEventLabels();
        eventLabels.setEventLabels(new String[0]);
        parametersMap.put(ZiggyEventLabels.class, eventLabels);
        taskConfig.setTaskDirectoryRegex("");
        List<UnitOfWork> unitsOfWork = new DataReceiptUnitOfWorkGenerator()
            .generateTasks(parametersMap);
        assertEquals(1, unitsOfWork.size());
        assertEquals("", unitsOfWork.get(0).getParameter("directory").getString());

    }
}
