package gov.nasa.ziggy.services.events;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.DATA_RECEIPT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.PIPELINE_HOME_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.TestEventDetector;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DatastoreConfigurationImporter;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionImporter;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.xml.ParameterImportExportOperations;
import gov.nasa.ziggy.pipeline.xml.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import jakarta.xml.bind.JAXBException;

/**
 * Unit tests for {@link ZiggyEventHandler}, {@link ZiggyEventStatus}, {@link ZiggyEvent}, and
 * {@link ZiggyEventHandlerFile} classes.
 *
 * @author PT
 */
public class ZiggyEventHandlerTest {

    public static final String TEST_DATA_DIR = "events";
    public static final String TEST_DATA_SRC = TEST_DATA.resolve("EventPipeline").toString();

    private Path testDataDir;
    private ZiggyEventHandler ziggyEventHandler;
    private String pipelineName = "sample";
    private Path readyIndicator1, readyIndicator2a, readyIndicator2b;
    private PipelineDefinitionOperations pipelineDefinitionOperations = Mockito
        .spy(PipelineDefinitionOperations.class);
    private PipelineExecutor pipelineExecutor = Mockito.spy(PipelineExecutor.class);
    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private TestOperations testOperations = new TestOperations();

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyPropertyRule pipelineHomeDirPropertyRule = new ZiggyPropertyRule(PIPELINE_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    public ZiggyPropertyRule dataReceiptDirPropertyRule = new ZiggyPropertyRule(DATA_RECEIPT_DIR,
        directoryRule, TEST_DATA_DIR);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(dataReceiptDirPropertyRule);

    @Before
    public void setUp() throws IOException {
        testDataDir = Paths.get(dataReceiptDirPropertyRule.getValue());
        testDataDir.toFile().mkdirs();
        readyIndicator1 = testDataDir.resolve("gazelle.READY.mammal.1");
        readyIndicator2a = testDataDir.resolve("psittacus.READY.bird.2");
        readyIndicator2b = testDataDir.resolve("archosaur.READY.bird.2");

        // Create the directories: they need to be there to get the DR UOW generator to
        // do the right thing.
        Files.createDirectory(testDataDir.resolve("gazelle"));
        Files.createDirectory(testDataDir.resolve("psittacus"));
        Files.createDirectory(testDataDir.resolve("archosaur"));

        // Create a ZiggyEventHandler instance with mocked instances of some
        // pipeline classes and a shortened interval between checks for the ready-indicator
        // file.
        ziggyEventHandler = Mockito.spy(ZiggyEventHandler.class);
        Mockito.doReturn(100L).when(ziggyEventHandler).readyFileCheckIntervalMillis();
        Mockito.doReturn(Mockito.mock(AlertService.class)).when(ziggyEventHandler).alertService();

        // Mock out the machinery that returns a PipelineExecutor so that we can substitute
        // our own UOW generator retrieval; for some reason the test fails when retrieving the
        // UOW for data receipt, but it works correctly in real life, so... .
        Mockito.doReturn(pipelineDefinitionOperations)
            .when(ziggyEventHandler)
            .pipelineDefinitionOperations();
        Mockito.doReturn(pipelineExecutor).when(ziggyEventHandler).pipelineExecutor();

        ziggyEventHandler.setPipelineName(pipelineName);
        ziggyEventHandler.setDirectory(testDataDir.toString());
        ziggyEventHandler.setName("test-event");

        // Set up the database, including with the pipeline to be used by the event handler.
        new ParameterImportExportOperations().importParameterLibrary(
            new File(TEST_DATA_SRC, "pl-event.xml"), null, ParamIoMode.STANDARD);
        new DatastoreConfigurationImporter(
            ImmutableList.of(new File(TEST_DATA_SRC, "pt-event.xml").toString()), false)
                .importConfiguration();
        new PipelineModuleDefinitionOperations().createDataReceiptPipelineModule();
        new PipelineDefinitionImporter()
            .importPipelineConfiguration(ImmutableList.of(new File(TEST_DATA_SRC, "pd-event.xml")));

        // Construct the PipelineSupervisor just so that we have a value set for the
        // number of workers
        new PipelineSupervisor(1, 1000);
    }

    @Test
    public void testSchema() throws IOException {
        Path schemaPath = Paths.get(
            ZiggyConfiguration.getInstance().getString(ZIGGY_HOME_DIR.property()), "schema", "xml",
            new ZiggyEventHandlerFile().getXmlSchemaFilename());
        List<String> schemaContent = Files.readAllLines(schemaPath, ZiggyFileUtils.ZIGGY_CHARSET);

        assertContains(schemaContent,
            "<xs:element name=\"pipelineEventDefinition\" type=\"ziggyEventHandlerFile\"/>");

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"ziggyEventHandlerFile\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"pipelineEvent\" type=\"ziggyEventHandler\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"ziggyEventHandler\"");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"enableOnClusterStart\" type=\"xs:boolean\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"directory\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"pipelineName\" type=\"xs:string\" use=\"required\"/>");
    }

    @Test
    public void testReadXml() throws InstantiationException, IllegalAccessException, SAXException,
        JAXBException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
        SecurityException {
        ValidatingXmlManager<ZiggyEventHandlerFile> xmlManager = new ValidatingXmlManager<>(
            ZiggyEventHandlerFile.class);
        ZiggyEventHandlerFile eventFile = xmlManager
            .unmarshal(Paths.get(TEST_DATA_SRC, "pe-test.xml").toFile());
        assertEquals(2, eventFile.getZiggyEventHandlers().size());
        for (ZiggyEventHandler handler : eventFile.getZiggyEventHandlers()) {
            validateEventHandler(handler);
        }
    }

    @Test
    public void testStartPipeline() throws IOException, InterruptedException {

        // At this point, there should be no entries in the events database table
        assertNull(parametersOperations.parameterSet("test-event mammal"));
        assertTrue(testOperations.allPipelineTasks().isEmpty());
        assertTrue(testOperations.allZiggyEvents().isEmpty());
        assertTrue(pipelineInstanceOperations.pipelineInstances().isEmpty());

        // create the ready-indicator file
        Files.createFile(readyIndicator1);

        // Create the manifest file.
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("gazelle")
            .resolve("test-manifest.xml"));

        ziggyEventHandler.run();

        List<ZiggyEvent> events = testOperations.allZiggyEvents();
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);

        // Get the task out of the database and check its values.
        List<PipelineTask> tasks = retrievePipelineTasks();

        assertEquals(1, tasks.size());
        PipelineTask task = tasks.get(0);
        UnitOfWork uow = task.getUnitOfWork();
        assertEquals(
            DirectoryProperties.dataReceiptDir().toAbsolutePath().resolve("gazelle").toString(),
            DirectoryUnitOfWorkGenerator.directory(uow));

        // The ready indicator file should be gone
        assertFalse(Files.exists(readyIndicator1));

        // Re-create the ready-indicator file to see that the pipeline gets
        // fired again
        Files.createFile(readyIndicator1);
        ziggyEventHandler.run();
        TestEventDetector.detectTestEvent(100L, () -> testOperations.allZiggyEvents().size() == 2);
    }

    private List<PipelineTask> retrievePipelineTasks() {
        return testOperations.allPipelineTasksForInstanceId(1L);
    }

    @Test
    public void testPreExistingReadyFile() throws IOException, InterruptedException {

        // create the ready-indicator file
        Files.createFile(readyIndicator1);

        // Create a manifest in the data receipt directory.
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("gazelle")
            .resolve("test-manifest.xml"));

        // There should be no indication that the event handler acted.
        assertNull(parametersOperations.parameterSet("test-event mammal"));
        assertTrue(testOperations.allPipelineTasks().isEmpty());
        assertTrue(testOperations.allZiggyEvents().isEmpty());
        assertTrue(pipelineInstanceOperations.pipelineInstances().isEmpty());

        ziggyEventHandler.run();

        List<ZiggyEvent> events = testOperations.allZiggyEvents();

        // Now we should see an event in the database.
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);
    }

    @Test
    public void testExceptionInRunnable() throws IOException, InterruptedException {

        // Note that what we'd really like to do is to have an exception thrown by
        // startPipeline(), but that's a private method that therefore can't be
        // mocked. This is the next best thing -- a PipelineException that occurs
        // elsewhere in the run() method (since all of the run() method is inside a try-block,
        // including startPipeline()).
        Mockito.doThrow(PipelineException.class).when(ziggyEventHandler).readyFilesForExecution();

        // create the ready-indicator file
        Files.createFile(readyIndicator1);
        ziggyEventHandler.run();

        // The ready-indicator file should still be present and the event handler should
        // be disabled
        assertTrue(Files.exists(readyIndicator1));
        assertFalse(ziggyEventHandler.isRunning());
    }

    @Test
    public void testEventWithTwoReadyFiles() throws IOException, InterruptedException {

        // create one ready-indicator file
        Files.createFile(readyIndicator2a);

        // Create the manifest file.
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("psittacus")
            .resolve("test-manifest.xml"));

        ziggyEventHandler.run();

        // At this point, there should be no entries in the events database table
        assertNull(parametersOperations.parameterSet("test-event mammal"));
        assertTrue(testOperations.allPipelineTasks().isEmpty());
        assertTrue(testOperations.allZiggyEvents().isEmpty());
        assertTrue(pipelineInstanceOperations.pipelineInstances().isEmpty());

        // When the second one is created, the event handler should act.
        Files.createFile(readyIndicator2b);

        // Create the manifest file.
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("archosaur")
            .resolve("test-manifest.xml"));

        ziggyEventHandler.run();

        List<ZiggyEvent> events = testOperations.allZiggyEvents();
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);

        // Get the tasks out of the database and check their values.
        List<PipelineTask> tasks = retrievePipelineTasks();

        assertEquals(2, tasks.size());
        List<String> uowStrings = new ArrayList<>();
        for (PipelineTask task : tasks) {
            UnitOfWork uow = task.getUnitOfWork();
            uowStrings.add(DirectoryUnitOfWorkGenerator.directory(uow));
        }
        Path dataReceiptDir = DirectoryProperties.dataReceiptDir().toAbsolutePath();
        assertTrue(uowStrings.contains(dataReceiptDir.resolve("psittacus").toString()));
        assertTrue(uowStrings.contains(dataReceiptDir.resolve("archosaur").toString()));
    }

    @Test
    public void testSimultaneousEvents() throws IOException, InterruptedException {

        Files.createFile(readyIndicator2a);
        Files.createFile(readyIndicator2b);
        Files.createFile(readyIndicator1);

        // Create a manifest in each data receipt directory.
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("gazelle")
            .resolve("test-manifest.xml"));
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("psittacus")
            .resolve("test-manifest.xml"));
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("archosaur")
            .resolve("test-manifest.xml"));

        ziggyEventHandler.run();

        List<ZiggyEvent> events = testOperations.allZiggyEvents();
        assertEquals(2, events.size());
    }

    @Test
    public void testRetrieveByInstance() throws IOException, InterruptedException {

        Files.createFile(readyIndicator2a);
        Files.createFile(readyIndicator2b);
        Files.createFile(readyIndicator1);

        // Create the manifest files.
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("psittacus")
            .resolve("test-manifest.xml"));
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("gazelle")
            .resolve("test-manifest.xml"));
        Files.createFile(Paths
            .get(ZiggyConfiguration.getInstance()
                .getString(PropertyName.DATA_RECEIPT_DIR.property()))
            .resolve("archosaur")
            .resolve("test-manifest.xml"));

        ziggyEventHandler.run();

        List<PipelineInstance> instances = pipelineInstanceOperations.pipelineInstances();
        instances.sort(Comparator.comparing(PipelineInstance::getId));
        List<ZiggyEvent> events = testOperations
            .ziggyEventsForPipelineInstances(List.of(instances.get(0)));
        assertEquals(1, events.size());
        assertEquals(1L, events.get(0).getPipelineInstanceId());
        assertEquals("test-event", events.get(0).getEventHandlerName());
        events = testOperations.ziggyEventsForPipelineInstances(List.of(instances.get(1)));
        assertEquals(1, events.size());
        assertEquals(2L, events.get(0).getPipelineInstanceId());
        assertEquals("test-event", events.get(0).getEventHandlerName());
        events = testOperations.ziggyEventsForPipelineInstances(instances);
        assertEquals(2, events.size());
        assertEquals(1L, events.get(0).getPipelineInstanceId());
        assertEquals(2L, events.get(1).getPipelineInstanceId());
        assertEquals("test-event", events.get(0).getEventHandlerName());
        assertEquals("test-event", events.get(1).getEventHandlerName());
    }

    @Test
    public void testNullEventLabel() throws IOException, InterruptedException {

        // create one ready-indicator file.
        Files.createFile(testDataDir.resolve("READY.mammal.1"));

        // Create a manifest in the data receipt directory.
        Files
            .createFile(Paths
                .get(ZiggyConfiguration.getInstance()
                    .getString(PropertyName.DATA_RECEIPT_DIR.property()))
                .resolve("test-manifest.xml"));
        ziggyEventHandler.run();

        List<ZiggyEvent> events = testOperations.allZiggyEvents();
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);

        // Get the instance out of the database and check its values.
        PipelineInstance instance = pipelineInstanceOperations.pipelineInstance(1L);
        assertEquals(0, pipelineInstanceOperations.parameterSets(instance).size());

        // Get the task out of the database and check its values.
        List<PipelineTask> tasks = retrievePipelineTasks();

        assertEquals(1, tasks.size());
        PipelineTask task = tasks.get(0);
        UnitOfWork uow = task.getUnitOfWork();
        assertEquals(directoryRule.directory().toAbsolutePath().resolve("events").toString(),
            DirectoryUnitOfWorkGenerator.directory(uow));
    }

    private void validateEventHandler(ZiggyEventHandler handler) {

        switch (handler.getName()) {
            case "data-receipt":
                validateDataReceiptHandler(handler);
                break;
            case "another-event-handler":
                validateOtherHandler(handler);
                break;
            default:
                throw new PipelineException("Unknown handler " + handler.getName());
        }
    }

    private void validateDataReceiptHandler(ZiggyEventHandler handler) {
        assertEquals("pipeline1", handler.getPipelineName());
        assertTrue(handler.isEnableOnClusterStart());
        assertFalse(handler.isRunning());
        assertEquals("/some/directory/or/other", handler.getDirectory());
    }

    private void validateOtherHandler(ZiggyEventHandler handler) {
        assertEquals("pipeline2", handler.getPipelineName());
        assertFalse(handler.isEnableOnClusterStart());
        assertFalse(handler.isRunning());
        assertEquals("/yet/another/directory", handler.getDirectory());
    }

    private static class TestOperations extends DatabaseOperations {

        public List<PipelineTask> allPipelineTasks() {
            return performTransaction(() -> new PipelineTaskCrud().retrieveAll());
        }

        public List<ZiggyEvent> allZiggyEvents() {
            return performTransaction(() -> new ZiggyEventCrud().retrieveAllEvents());
        }

        public List<ZiggyEvent> ziggyEventsForPipelineInstances(List<PipelineInstance> instances) {
            return performTransaction(() -> new ZiggyEventCrud().retrieve(instances));
        }

        public List<PipelineTask> allPipelineTasksForInstanceId(long instanceId) {
            return performTransaction(
                () -> new PipelineTaskCrud().retrieveTasksForInstance(instanceId));
        }
    }
}
