package gov.nasa.ziggy.uow;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
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

/**
 * Unit test class for DefaultUnitOfWork.
 *
 * @author PT
 */
public class DatastoreDirectoryUnitOfWorkTest {

    private Path datastoreRoot;
    private Map<Class<? extends ParametersInterface>, ParametersInterface> parametersMap;
    private TaskConfigurationParameters taskConfigurationParameters;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(datastoreRootDirPropertyRule);

    @Before
    public void setup() {

        datastoreRoot = directoryRule.directory();
        // Create the datastore.
        File datastore = datastoreRoot.toFile();

        // create some directories within the datastore
        File sector0001 = new File(datastore, "sector-0001");
        sector0001.mkdirs();
        File sector0002 = new File(datastore, "sector-0002");
        sector0002.mkdirs();

        File cal0001 = new File(sector0001, "cal");
        cal0001.mkdirs();
        File cal0002 = new File(sector0002, "cal");
        cal0002.mkdirs();
        File pa0002 = new File(sector0002, "pa");
        pa0002.mkdirs();

        File ccd11 = new File(cal0001, "ccd-1:1");
        ccd11.mkdirs();
        File ccd12 = new File(cal0001, "ccd-1:2");
        ccd12.mkdirs();
        File ccd21 = new File(cal0001, "ccd-2:1");
        ccd21.mkdirs();
        ccd11 = new File(cal0002, "ccd-1:1");
        ccd11.mkdirs();
        ccd12 = new File(cal0002, "ccd-1:2");
        ccd12.mkdirs();
        ccd21 = new File(cal0002, "ccd-2:1");
        ccd21.mkdirs();
        ccd11 = new File(pa0002, "ccd-1:1");
        ccd11.mkdirs();
        ccd12 = new File(pa0002, "ccd-1:2");
        ccd12.mkdirs();
        ccd21 = new File(pa0002, "ccd-2:1");
        ccd21.mkdirs();

        // Construct the task configuration parameters and the parameters map
        taskConfigurationParameters = new TaskConfigurationParameters();
        taskConfigurationParameters.setSingleSubtask(false);
        taskConfigurationParameters.setTaskDirectoryRegex("(sector-[0-9]{4})/cal/ccd-(1:[1234])");
        parametersMap = new HashMap<>();
        parametersMap.put(TaskConfigurationParameters.class, taskConfigurationParameters);
    }

    /**
     * Basic functionality test -- makes sure that the expected behavior in terms of included and
     * excluded directories, brief states, etc., is obtained.
     */
    @Test
    public void testGenerateUnitsOfWork() {

        DatastoreDirectoryUnitOfWorkGenerator uowGenInstance = new DatastoreDirectoryUnitOfWorkGenerator();
        List<UnitOfWork> uowList = uowGenInstance.generateUnitsOfWork(parametersMap);
        assertEquals(4, uowList.size());

        // construct a map of expected results
        Map<String, String> uowMap = new HashMap<>();
        for (UnitOfWork uow : uowList) {
            uowMap.put(DirectoryUnitOfWorkGenerator.directory(uow), uow.briefState());
            assertFalse(DatastoreDirectoryUnitOfWorkGenerator.singleSubtask(uow));
        }
        Set<String> uowKeys = uowMap.keySet();

        // check for the expected results
        assertTrue(uowKeys.contains("sector-0001/cal/ccd-1:1"));
        assertEquals("sector-0001,1:1", uowMap.get("sector-0001/cal/ccd-1:1"));
        assertTrue(uowKeys.contains("sector-0002/cal/ccd-1:1"));
        assertEquals("sector-0002,1:1", uowMap.get("sector-0002/cal/ccd-1:1"));
        assertTrue(uowKeys.contains("sector-0001/cal/ccd-1:2"));
        assertEquals("sector-0001,1:2", uowMap.get("sector-0001/cal/ccd-1:2"));
        assertTrue(uowKeys.contains("sector-0002/cal/ccd-1:2"));
        assertEquals("sector-0002,1:2", uowMap.get("sector-0002/cal/ccd-1:2"));
    }

    /**
     * Tests the generation of tasks that will have a single subtask
     */
    @Test
    public void testGenerateTasksSingleSubtask() {

        DatastoreDirectoryUnitOfWorkGenerator uowGenInstance = new DatastoreDirectoryUnitOfWorkGenerator();
        taskConfigurationParameters.setSingleSubtask(true);
        List<UnitOfWork> uowList = uowGenInstance.generateUnitsOfWork(parametersMap);
        for (UnitOfWork uow : uowList) {
            assertTrue(DatastoreDirectoryUnitOfWorkGenerator.singleSubtask(uow));
        }
    }

    /**
     * Tests the generation of tasks for which the "brief state" is the full directory
     */
    @Test
    public void testGenerateFullBriefState() {

        DatastoreDirectoryUnitOfWorkGenerator uowGenInstance = new DatastoreDirectoryUnitOfWorkGenerator();
        taskConfigurationParameters.setTaskDirectoryRegex("sector-[0-9]{4}/cal/ccd-1:[1234]");
        List<UnitOfWork> uowList = uowGenInstance.generateUnitsOfWork(parametersMap);

        assertEquals(4, uowList.size());

        Map<String, String> uowMap = new HashMap<>();
        for (UnitOfWork uow : uowList) {
            uowMap.put(DirectoryUnitOfWorkGenerator.directory(uow), uow.briefState());
            assertFalse(DatastoreDirectoryUnitOfWorkGenerator.singleSubtask(uow));
        }
        Set<String> uowKeys = uowMap.keySet();

        // check for the expected results
        assertTrue(uowKeys.contains("sector-0001/cal/ccd-1:1"));
        assertEquals("sector-0001/cal/ccd-1:1", uowMap.get("sector-0001/cal/ccd-1:1"));
        assertTrue(uowKeys.contains("sector-0002/cal/ccd-1:1"));
        assertEquals("sector-0002/cal/ccd-1:1", uowMap.get("sector-0002/cal/ccd-1:1"));
        assertTrue(uowKeys.contains("sector-0001/cal/ccd-1:2"));
        assertEquals("sector-0001/cal/ccd-1:2", uowMap.get("sector-0001/cal/ccd-1:2"));
        assertTrue(uowKeys.contains("sector-0002/cal/ccd-1:2"));
        assertEquals("sector-0002/cal/ccd-1:2", uowMap.get("sector-0002/cal/ccd-1:2"));
    }
}
