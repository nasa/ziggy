package gov.nasa.ziggy.uow;

import static gov.nasa.ziggy.services.config.PropertyName.DATA_RECEIPT_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.services.events.ZiggyEventLabels;

/**
 * Unit tests for the {@link DataReceiptUnitOfWorkGenerator} class.
 *
 * @author PT
 */
public class DataReceiptUnitOfWorkGeneratorTest {

    private Path dataImporterPath;
    Map<Class<? extends ParametersInterface>, ParametersInterface> parametersMap;
    TaskConfigurationParameters taskConfig;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule dataReceiptDirPropertyRule = new ZiggyPropertyRule(DATA_RECEIPT_DIR,
        directoryRule, "data-import");

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(dataReceiptDirPropertyRule);

    @Before
    public void setUp() throws IOException {

        dataImporterPath = Paths.get(dataReceiptDirPropertyRule.getProperty());
        // Create the data receipt main directory.
        dataImporterPath.toFile().mkdirs();

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
