package gov.nasa.ziggy.services.events;

import static gov.nasa.ziggy.pipeline.definition.XmlUtils.assertContains;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.complexTypeContent;
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
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.hibernate.Hibernate;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.data.management.DataFileTypeImporter;
import gov.nasa.ziggy.data.management.DataReceiptPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit tests for {@link ZiggyEventHandler}, {@link ZiggyEventStatus}, {@link ZiggyEvent}, and
 * {@link ZiggyEventHandlerFile} classes.
 *
 * @author PT
 */
public class ZiggyEventHandlerTest {

    @Rule
    public ZiggyDirectoryRule dirRule = new ZiggyDirectoryRule();

    public static final String TEST_DATA_DIR = "events";
    public static final String TEST_DATA_SRC = "test/data/EventPipeline";

    public static long testStatusSleepTime;

    private Path testDataDir;
    private ZiggyEventHandler ziggyEventHandler;
    private PipelineDefinitionName pipelineName = new PipelineDefinitionName("sample");
    private String testInstanceName = "test-instance-name";
    private Path readyIndicator1, readyIndicator2a, readyIndicator2b;
    private PipelineOperations pipelineOperations = Mockito.spy(PipelineOperations.class);
    private PipelineExecutor pipelineExecutor = Mockito.spy(PipelineExecutor.class);

    @Before
    public void setUp() throws IOException {
        testStatusSleepTime = 200L;
        testDataDir = dirRule.testDirPath().resolve(TEST_DATA_DIR);
        testDataDir.toFile().mkdirs();
        readyIndicator1 = testDataDir.resolve("gazelle.READY.mammal.1");
        readyIndicator2a = testDataDir.resolve("psittacus.READY.bird.2");
        readyIndicator2b = testDataDir.resolve("archosaur.READY.bird.2");
        System.setProperty(PropertyNames.DATA_RECEIPT_DIR_PROP_NAME, testDataDir.toString());

        // Create the directories: they need to be there to get the DR UOW generator to
        // do the right thing.
        Files.createDirectory(testDataDir.resolve("gazelle"));
        Files.createDirectory(testDataDir.resolve("psittacus"));
        Files.createDirectory(testDataDir.resolve("archosaur"));

        String workingDir = System.getProperty("user.dir");
        Path distDirPath = Paths.get(workingDir, "build");
        System.setProperty("ziggy.home.dir", distDirPath.toString());
        ClassWrapper<UnitOfWorkGenerator> dataReceiptUowGenerator = new ClassWrapper<>(
            DataReceiptUnitOfWorkGenerator.class);

        // Create a ZiggyEventHandler instance with mocked instances of some
        // pipeline classes and a shortened interval between checks for the ready-indicator
        // file.
        ziggyEventHandler = Mockito.spy(ZiggyEventHandler.class);
        Mockito.doReturn(testInstanceName).when(ziggyEventHandler).instanceName();
        Mockito.doReturn(100L).when(ziggyEventHandler).readyFileCheckIntervalMillis();
        Mockito.doReturn(Mockito.mock(AlertService.class)).when(ziggyEventHandler).alertService();
        Mockito.doReturn(new CompositeConfiguration()).when(ziggyEventHandler).configuration();

        // Mock out the machinery that returns a PipelineExecutor so that we can substitute
        // our own UOW generator retrieval; for some reason the test fails when retrieving the
        // UOW for data receipt, but it works correctly in real life, so... .
        Mockito.doReturn(pipelineOperations).when(ziggyEventHandler).pipelineOperations();
        Mockito.doReturn(pipelineExecutor).when(pipelineOperations).pipelineExecutor();
        Mockito.doReturn(dataReceiptUowGenerator)
            .when(pipelineExecutor)
            .unitOfWorkGenerator(ArgumentMatchers.any(PipelineDefinitionNode.class));

        // Don't allow the event handler to actually send tasks for the worker to start.
        Mockito.doNothing()
            .when(ziggyEventHandler)
            .sendWorkerMessageForTasks(ArgumentMatchers.anyList());

        ziggyEventHandler.setPipelineName(pipelineName);
        ziggyEventHandler.setDirectory(testDataDir.toString());
        ziggyEventHandler.setName("test-event");

        // Set up the database, including with the pipeline to be used by the event handler.
        ZiggyUnitTestUtils.setUpDatabase();
        DatabaseTransactionFactory.performTransaction(() -> {
            new ParametersOperations().importParameterLibrary(
                new File(TEST_DATA_SRC, "pl-event.xml"), null, ParamIoMode.STANDARD);
            new DataFileTypeImporter(
                ImmutableList.of(new File(TEST_DATA_SRC, "pt-event.xml").toString()), false)
                    .importFromFiles();
            new PipelineModuleDefinitionCrud()
                .createOrUpdate(DataReceiptPipelineModule.createDataReceiptPipelineForDb());
            new PipelineDefinitionOperations().importPipelineConfiguration(
                ImmutableList.of(new File(TEST_DATA_SRC, "pd-event.xml")));
            return null;
        });
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        System.clearProperty("ziggy.home.dir");
        System.clearProperty(PropertyNames.DATA_RECEIPT_DIR_PROP_NAME);
        ZiggyUnitTestUtils.tearDownDatabase();
    }

    @Test
    public void testSchema() throws IOException {
        Path schemaPath = Paths.get(System.getProperty("ziggy.home.dir"), "schema", "xml",
            new ZiggyEventHandlerFile().getXmlSchemaFilename());
        List<String> schemaContent = Files.readAllLines(schemaPath);

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
        jakarta.xml.bind.JAXBException, IllegalArgumentException, InvocationTargetException,
        NoSuchMethodException, SecurityException {
        ValidatingXmlManager<ZiggyEventHandlerFile> xmlManager = new ValidatingXmlManager<>(
            ZiggyEventHandlerFile.class);
        ZiggyEventHandlerFile eventFile = xmlManager
            .unmarshal(Paths.get(TEST_DATA_SRC, "pe-test.xml").toFile());
        assertEquals(2, eventFile.getZiggyEventHandlers().size());
        for (ZiggyEventHandler handler : eventFile.getZiggyEventHandlers()) {
            validateEventHandler(handler);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStartPipeline() throws IOException, InterruptedException {

        // At this point, there should be no entries in the events database table
        DatabaseTransactionFactory.performTransaction(() -> {
            assertTrue(new ZiggyEventCrud().retrieveAllEvents().isEmpty());
            assertTrue(new PipelineInstanceCrud().retrieveAll().isEmpty());
            assertTrue(new PipelineTaskCrud().retrieveAll().isEmpty());
            assertNull(new ParameterSetCrud()
                .retrieveLatestVersionForName(new ParameterSetName("test-event mammal")));
            return null;
        });

        // create the ready-indicator file
        Files.createFile(readyIndicator1);
        ziggyEventHandler.run();

        List<ZiggyEvent> events = (List<ZiggyEvent>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<ZiggyEvent> ziggyEvents = new ZiggyEventCrud().retrieveAllEvents();
                return ziggyEvents;
            });
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName().getName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);

        // Get the instance out of the database and check its values.
        PipelineInstance instance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineInstance pipelineInstance = new PipelineInstanceCrud().retrieve(1L);
                Hibernate.initialize(pipelineInstance.getPipelineParameterSets());
                return pipelineInstance;
            });

        assertEquals(testInstanceName, instance.getName());
        assertEquals(1, instance.getPipelineParameterSets().size());
        ParameterSet parameterSet = instance.getPipelineParameterSets()
            .get(new ClassWrapper<>(ZiggyEventLabels.class));
        ZiggyEventLabels eventLabels = (ZiggyEventLabels) parameterSet.getParameters()
            .getInstance();
        assertEquals("test-event", eventLabels.getEventHandlerName());
        assertEquals("mammal", eventLabels.getEventName());
        assertEquals(1, eventLabels.getEventLabels().length);
        assertEquals("gazelle", eventLabels.getEventLabels()[0]);

        // Get the task out of the database and check its values.
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<PipelineTask> pipelineTasks = new PipelineTaskCrud()
                    .retrieveTasksForInstance(1L);
                Hibernate.initialize(
                    pipelineTasks.get(0).getPipelineInstanceNode().getModuleParameterSets());
                return pipelineTasks;
            });

        assertEquals(1, tasks.size());
        PipelineTask task = tasks.get(0);
        UnitOfWork uow = task.getUowTask().getInstance();
        assertEquals("gazelle", DirectoryUnitOfWorkGenerator.directory(uow));
        ParameterSet labelsParamSet = task.getParameterSet(ZiggyEventLabels.class);
        eventLabels = (ZiggyEventLabels) labelsParamSet.getParameters().getInstance();
        assertEquals("test-event", eventLabels.getEventHandlerName());
        assertEquals("mammal", eventLabels.getEventName());
        assertEquals(1, eventLabels.getEventLabels().length);
        assertEquals("gazelle", eventLabels.getEventLabels()[0]);

        // The ready indicator file should be gone
        assertFalse(Files.exists(readyIndicator1));

        // Re-create the ready-indicator file to see that the pipeline gets
        // fired again
        Files.createFile(readyIndicator1);
        ziggyEventHandler.run();
        events = (List<ZiggyEvent>) DatabaseTransactionFactory.performTransaction(() -> {
            List<ZiggyEvent> ziggyEvents = new ZiggyEventCrud().retrieveAllEvents();
            return ziggyEvents;
        });
        assertEquals(2, events.size());
    }

    @Test
    public void testPreExistingReadyFile() throws IOException, InterruptedException {

        // create the ready-indicator file
        Files.createFile(readyIndicator1);

        // There should be no indication that the event handler acted.
        DatabaseTransactionFactory.performTransaction(() -> {
            assertTrue(new ZiggyEventCrud().retrieveAllEvents().isEmpty());
            assertTrue(new PipelineInstanceCrud().retrieveAll().isEmpty());
            assertTrue(new PipelineTaskCrud().retrieveAll().isEmpty());
            assertNull(new ParameterSetCrud()
                .retrieveLatestVersionForName(new ParameterSetName("test-event mammal")));
            return null;
        });

        ziggyEventHandler.run();

        @SuppressWarnings("unchecked")
        List<ZiggyEvent> events = (List<ZiggyEvent>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<ZiggyEvent> ziggyEvents = new ZiggyEventCrud().retrieveAllEvents();
                return ziggyEvents;
            });

        // Now we should see an event in the database.
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName().getName());
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
        ziggyEventHandler.run();

        // At this point, there should be no entries in the events database table
        DatabaseTransactionFactory.performTransaction(() -> {
            assertTrue(new ZiggyEventCrud().retrieveAllEvents().isEmpty());
            assertTrue(new PipelineInstanceCrud().retrieveAll().isEmpty());
            assertTrue(new PipelineTaskCrud().retrieveAll().isEmpty());
            assertNull(new ParameterSetCrud()
                .retrieveLatestVersionForName(new ParameterSetName("test-event mammal")));
            return null;
        });

        // When the second one is created, the event handler should act.
        Files.createFile(readyIndicator2b);
        ziggyEventHandler.run();

        @SuppressWarnings("unchecked")
        List<ZiggyEvent> events = (List<ZiggyEvent>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<ZiggyEvent> ziggyEvents = new ZiggyEventCrud().retrieveAllEvents();
                return ziggyEvents;
            });
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName().getName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);

        // Get the instance out of the database and check its values.
        PipelineInstance instance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineInstance pipelineInstance = new PipelineInstanceCrud().retrieve(1L);
                Hibernate.initialize(pipelineInstance.getPipelineParameterSets());
                return pipelineInstance;
            });

        assertEquals(testInstanceName, instance.getName());
        assertEquals(1, instance.getPipelineParameterSets().size());
        ParameterSet parameterSet = instance.getPipelineParameterSets()
            .get(new ClassWrapper<>(ZiggyEventLabels.class));
        ZiggyEventLabels eventLabels = (ZiggyEventLabels) parameterSet.getParameters()
            .getInstance();
        assertEquals("test-event", eventLabels.getEventHandlerName());
        assertEquals("bird", eventLabels.getEventName());
        Set<String> labels = Sets.newHashSet(eventLabels.getEventLabels());
        assertEquals(2, labels.size());
        assertTrue(labels.contains("psittacus"));
        assertTrue(labels.contains("archosaur"));

        // Get the tasks out of the database and check their values.
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<PipelineTask> pipelineTasks = new PipelineTaskCrud()
                    .retrieveTasksForInstance(1L);
                Hibernate.initialize(
                    pipelineTasks.get(0).getPipelineInstanceNode().getModuleParameterSets());
                return pipelineTasks;
            });

        assertEquals(2, tasks.size());
        List<String> uowStrings = new ArrayList<>();
        for (PipelineTask task : tasks) {
            UnitOfWork uow = task.getUowTask().getInstance();
            uowStrings.add(DirectoryUnitOfWorkGenerator.directory(uow));
            ParameterSet labelsParamSet = task.getParameterSet(ZiggyEventLabels.class);
            eventLabels = (ZiggyEventLabels) labelsParamSet.getParameters().getInstance();
            assertEquals("test-event", eventLabels.getEventHandlerName());
            assertEquals("bird", eventLabels.getEventName());
            labels = Sets.newHashSet(eventLabels.getEventLabels());
            assertEquals(2, labels.size());
            assertTrue(labels.contains("psittacus"));
            assertTrue(labels.contains("archosaur"));
        }
        assertTrue(uowStrings.contains("psittacus"));
        assertTrue(uowStrings.contains("archosaur"));

    }

    @Test
    public void testSimultaneousEvents() throws IOException, InterruptedException {

        Files.createFile(readyIndicator2a);
        Files.createFile(readyIndicator2b);
        Files.createFile(readyIndicator1);

        ziggyEventHandler.run();

        @SuppressWarnings("unchecked")
        List<ZiggyEvent> events = (List<ZiggyEvent>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<ZiggyEvent> ziggyEvents = new ZiggyEventCrud().retrieveAllEvents();
                return ziggyEvents;
            });
        assertEquals(2, events.size());
    }

    @Test
    public void testNullEventLabel() throws IOException, InterruptedException {

        // Start by updating the task directory regex for data receipt to be "".
        DatabaseTransactionFactory.performTransaction(() -> {
            new ParametersOperations().importParameterLibrary(
                new File(TEST_DATA_SRC, "pl-event-override.xml"), null, ParamIoMode.STANDARD);
            return null;
        });

        // create one ready-indicator file.
        Files.createFile(testDataDir.resolve("null.READY.mammal.1"));
        ziggyEventHandler.run();

        @SuppressWarnings("unchecked")
        List<ZiggyEvent> events = (List<ZiggyEvent>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<ZiggyEvent> ziggyEvents = new ZiggyEventCrud().retrieveAllEvents();
                return ziggyEvents;
            });
        assertEquals(1, events.size());
        ZiggyEvent event = events.get(0);
        assertEquals(1L, event.getPipelineInstanceId());
        assertEquals("sample", event.getPipelineName().getName());
        assertEquals("test-event", event.getEventHandlerName());
        assertTrue(event.getEventTime() != null);

        // Get the instance out of the database and check its values.
        PipelineInstance instance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineInstance pipelineInstance = new PipelineInstanceCrud().retrieve(1L);
                Hibernate.initialize(pipelineInstance.getPipelineParameterSets());
                return pipelineInstance;
            });
        assertEquals(testInstanceName, instance.getName());
        assertEquals(1, instance.getPipelineParameterSets().size());
        ParameterSet parameterSet = instance.getPipelineParameterSets()
            .get(new ClassWrapper<>(ZiggyEventLabels.class));
        ZiggyEventLabels eventLabels = (ZiggyEventLabels) parameterSet.getParameters()
            .getInstance();
        assertEquals("test-event", eventLabels.getEventHandlerName());
        assertEquals("mammal", eventLabels.getEventName());
        assertEquals(0, eventLabels.getEventLabels().length);

        // Get the task out of the database and check its values.
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> {
                List<PipelineTask> pipelineTasks = new PipelineTaskCrud()
                    .retrieveTasksForInstance(1L);
                Hibernate.initialize(
                    pipelineTasks.get(0).getPipelineInstanceNode().getModuleParameterSets());
                return pipelineTasks;
            });

        assertEquals(1, tasks.size());
        PipelineTask task = tasks.get(0);
        UnitOfWork uow = task.getUowTask().getInstance();
        assertEquals("", DirectoryUnitOfWorkGenerator.directory(uow));
        ParameterSet labelsParamSet = task.getParameterSet(ZiggyEventLabels.class);
        eventLabels = (ZiggyEventLabels) labelsParamSet.getParameters().getInstance();
        assertEquals("test-event", eventLabels.getEventHandlerName());
        assertEquals("mammal", eventLabels.getEventName());
        assertEquals(0, eventLabels.getEventLabels().length);

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
        assertEquals("pipeline1", handler.getPipelineName().getName());
        assertTrue(handler.isEnableOnClusterStart());
        assertFalse(handler.isRunning());
        assertEquals("/some/directory/or/other", handler.getDirectory());
    }

    private void validateOtherHandler(ZiggyEventHandler handler) {
        assertEquals("pipeline2", handler.getPipelineName().getName());
        assertFalse(handler.isEnableOnClusterStart());
        assertFalse(handler.isRunning());
        assertEquals("/yet/another/directory", handler.getDirectory());
    }

}
