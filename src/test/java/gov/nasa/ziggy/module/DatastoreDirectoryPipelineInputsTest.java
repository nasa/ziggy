package gov.nasa.ziggy.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreFileManager;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.data.datastore.DatastoreTestUtils;
import gov.nasa.ziggy.data.datastore.DatastoreWalker;
import gov.nasa.ziggy.module.hdf5.Hdf5ModuleInterface;
import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModelMetadata;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Unit test class for {@link DatastoreDirectoryPipelineInputs}.
 *
 * @author PT
 */
public class DatastoreDirectoryPipelineInputsTest {

    private static final int EXPECTED_SUBTASK_COUNT = 7;
    private PipelineTask pipelineTask;
    private PipelineInstance pipelineInstance;
    private PipelineInstanceNode pipelineInstanceNode;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private PipelineInputsForTest pipelineInputs;
    private Path taskDirectory;
    private Map<String, DatastoreRegexp> regexpsByName;
    private DatastoreWalker datastoreWalker;
    private DatastoreFileManager datastoreFileManager;
    private Map<String, String> regexpValueByName = new HashMap<>();
    private ModelMetadata modelMetadata;
    private Map<String, Set<Path>> filesForSubtasks;
    private TaskConfiguration taskConfiguration;
    private DataFileType calibratedCollateralPixelDataFileType;

    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootProperty = new ZiggyPropertyRule(
        PropertyName.DATASTORE_ROOT_DIR, ziggyDirectoryRule, "datastore");

    public ZiggyPropertyRule taskDirRule = new ZiggyPropertyRule(PropertyName.RESULTS_DIR,
        ziggyDirectoryRule, "pipeline-results");

    @Rule
    public final RuleChain testRuleChain = RuleChain.outerRule(ziggyDirectoryRule)
        .around(datastoreRootProperty)
        .around(taskDirRule);

    @Before
    public void setup() throws IOException {

        pipelineTask = Mockito.mock(PipelineTask.class);
        taskDirectory = DirectoryProperties.taskDataDir();

        regexpsByName = DatastoreTestUtils.regexpsByName();
        datastoreWalker = new DatastoreWalker(regexpsByName,
            DatastoreTestUtils.datastoreNodesByFullPath());

        // Create datastore directories.
        DatastoreTestUtils.createDatastoreDirectories();

        // Get and update data file types.
        Map<String, DataFileType> dataFileTypes = DatastoreTestUtils.dataFileTypesByName();
        DataFileType uncalibratedSciencePixelDataFileType = dataFileTypes
            .get("uncalibrated science pixel values");
        uncalibratedSciencePixelDataFileType
            .setFileNameRegexp("uncalibrated-pixels-[0-9]+\\.science\\.nc");
        DataFileType uncalibratedCollateralPixelDataFileType = dataFileTypes
            .get("uncalibrated collateral pixel values");
        uncalibratedCollateralPixelDataFileType
            .setFileNameRegexp("uncalibrated-pixels-[0-9]+\\.collateral\\.nc");
        DataFileType allFilesAllSubtasksDataFileType = dataFileTypes
            .get("calibrated science pixel values");
        allFilesAllSubtasksDataFileType.setFileNameRegexp("everyone-needs-me-[0-9.nc");
        calibratedCollateralPixelDataFileType = dataFileTypes
            .get("calibrated collateral pixel values");
        calibratedCollateralPixelDataFileType
            .setFileNameRegexp("calibrated-pixels-[0-9]+\\.collateral\\.nc");

        // Construct the Map from regexp name to value.
        regexpValueByName.put("sector", "sector-0002");
        regexpValueByName.put("cadenceType", "target");
        regexpValueByName.put("channel", "1:1:A");
        for (Map.Entry<String, String> regexpEntry : regexpValueByName.entrySet()) {
            regexpsByName.get(regexpEntry.getKey()).setInclude(regexpEntry.getValue());
        }

        // Create datastore files.
        constructDatastoreFiles(uncalibratedSciencePixelDataFileType, EXPECTED_SUBTASK_COUNT + 1,
            "uncalibrated-pixels-", ".science.nc");
        constructDatastoreFiles(uncalibratedCollateralPixelDataFileType, EXPECTED_SUBTASK_COUNT,
            "uncalibrated-pixels-", ".collateral.nc");
        constructDatastoreFiles(allFilesAllSubtasksDataFileType, 2, "everyone-needs-me-", ".nc");

        // Construct a model type and model metadata.
        ModelType modelType = new ModelType();
        modelType.setType("test");
        modelMetadata = new ModelMetadata();
        modelMetadata.setModelType(modelType);
        modelMetadata.setOriginalFileName("foo");
        modelMetadata.setDatastoreFileName("bar");
        Files.createDirectories(modelMetadata.datastoreModelPath().getParent());
        Files.createFile(modelMetadata.datastoreModelPath());

        // Create the PipelineTask.
        pipelineTask = Mockito.mock(PipelineTask.class);
        pipelineInstance = Mockito.mock(PipelineInstance.class);
        pipelineInstanceNode = Mockito.mock(PipelineInstanceNode.class);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.when(pipelineTask.getModuleName()).thenReturn("testmod");
        Mockito.when(pipelineTask.getPipelineInstance()).thenReturn(pipelineInstance);
        Mockito.when(pipelineTask.getPipelineInstanceNode()).thenReturn(pipelineInstanceNode);
        Mockito.when(pipelineTask.pipelineDefinitionNode()).thenReturn(pipelineDefinitionNode);
        Mockito.when(pipelineInstanceNode.getPipelineDefinitionNode())
            .thenReturn(pipelineDefinitionNode);
        Mockito.when(pipelineDefinitionNode.getInputDataFileTypes())
            .thenReturn(Set.of(uncalibratedSciencePixelDataFileType,
                uncalibratedCollateralPixelDataFileType, allFilesAllSubtasksDataFileType));
        Mockito.when(pipelineDefinitionNode.getOutputDataFileTypes())
            .thenReturn(Set.of(calibratedCollateralPixelDataFileType));
        Mockito.when(pipelineDefinitionNode.getModelTypes()).thenReturn(Set.of(modelType));

        // Create the parameter sets.
        Mockito.when(pipelineInstance.getPipelineParameterSets())
            .thenReturn(pipelineParameterSets());
        Mockito.when(pipelineInstanceNode.getModuleParameterSets())
            .thenReturn(moduleParameterSets());

        // Construct the UOW.
        DatastoreDirectoryUnitOfWorkGenerator uowGenerator = Mockito
            .spy(DatastoreDirectoryUnitOfWorkGenerator.class);
        Mockito.doReturn(datastoreWalker).when(uowGenerator).datastoreWalker();
        List<UnitOfWork> uows = PipelineExecutor.generateUnitsOfWork(uowGenerator,
            pipelineInstanceNode);
        Mockito.when(pipelineTask.uowTaskInstance()).thenReturn(uows.get(0));

        // Construct mocked DatastoreFileManager.
        datastoreFileManager = Mockito.mock(DatastoreFileManager.class);
        Mockito.when(datastoreFileManager.taskDirectory()).thenReturn(taskDirectory);
        filesForSubtasks = new HashMap<>();
        populateFilesForSubtasks(EXPECTED_SUBTASK_COUNT);
        Mockito.when(datastoreFileManager.filesForSubtasks()).thenReturn(filesForSubtasks);
        Map<Path, String> modelFilesForTask = new HashMap<>();
        modelFilesForTask.put(modelMetadata.datastoreModelPath(), "foo");
        Mockito.when(datastoreFileManager.modelFilesForTask()).thenReturn(modelFilesForTask);
        Mockito
            .when(datastoreFileManager.copyDatastoreFilesToTaskDirectory(
                ArgumentMatchers.anyCollection(), ArgumentMatchers.anyMap()))
            .thenReturn(pathsBySubtaskDirectory(EXPECTED_SUBTASK_COUNT));

        // Construct the pipeline inputs. We can't use the standard method of a Mockito spy
        // applied to a PipelineInputs instance because Mockito's spy and the HDF5 API don't
        // work together. Hence we need to have a subclass of DatastoreDirectoryPipelineInputs
        // that takes all the necessary arguments and makes correct use of them.
        pipelineInputs = new PipelineInputsForTest(datastoreFileManager,
            Mockito.mock(AlertService.class), pipelineTask);

        taskConfiguration = new TaskConfiguration();
    }

    /** Constructs a collection of zero-length files in the datastore. */
    private void constructDatastoreFiles(DataFileType dataFileType, int fileCount,
        String filenamePrefix, String filenameSuffix) throws IOException {
        Path datastorePath = datastoreWalker.pathFromLocationAndRegexpValues(regexpValueByName,
            dataFileType.getLocation());
        for (int fileCounter = 0; fileCounter < fileCount; fileCounter++) {
            String filename = filenamePrefix + fileCounter + filenameSuffix;
            Files.createDirectories(datastorePath);
            Files.createFile(datastorePath.resolve(filename));
        }
    }

    private void populateFilesForSubtasks(int subtaskCount) {
        for (int subtaskIndex = 0; subtaskIndex < subtaskCount; subtaskIndex++) {
            String baseName = "uncalibrated-pixels-" + subtaskIndex;
            Set<Path> subtaskFiles = new HashSet<>();
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve("everyone-needs-me-0.nc"));
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("cal")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve("everyone-needs-me-1.nc"));
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("target")
                .resolve("science")
                .resolve("1:1:A")
                .resolve(baseName + ".science.nc"));
            subtaskFiles.add(DirectoryProperties.datastoreRootDir()
                .toAbsolutePath()
                .resolve("sector-0002")
                .resolve("mda")
                .resolve("dr")
                .resolve("pixels")
                .resolve("target")
                .resolve("collateral")
                .resolve("1:1:A")
                .resolve(baseName + ".collateral.nc"));
            filesForSubtasks.put(baseName, subtaskFiles);
        }
    }

    private Map<Path, Set<Path>> pathsBySubtaskDirectory(int subtaskCount) throws IOException {
        Map<Path, Set<Path>> pathsBySubtaskDirectory = new HashMap<>();
        for (int subtaskIndex = 0; subtaskIndex < subtaskCount; subtaskIndex++) {
            Path subtaskPath = taskDirectory.resolve("st-" + subtaskIndex);
            Files.createDirectories(subtaskPath);
            String baseName = "uncalibrated-pixels-" + subtaskIndex;
            pathsBySubtaskDirectory.put(subtaskPath, filesForSubtasks.get(baseName));
        }
        return pathsBySubtaskDirectory;
    }

    /** Exercises the copyDatastoreFilesToTaskDirectory() method. */
    @Test
    public void testCopyDatastoreFilesToTaskDirectory() throws IOException {

        // Note that we don't actually copy any files to the subtask directory.
        // That capability has been fully tested in the DatastoreFileManager.
        // Here we just want to see that the HDF5 file in each subtask directory
        // contains what we expect to see.
        pipelineInputs.copyDatastoreFilesToTaskDirectory(taskConfiguration, taskDirectory);
        assertEquals(EXPECTED_SUBTASK_COUNT, taskConfiguration.getSubtaskCount());
        Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();

        for (int subtaskIndex = 0; subtaskIndex < EXPECTED_SUBTASK_COUNT; subtaskIndex++) {
            assertTrue(Files
                .exists(taskDirectory.resolve("st-" + subtaskIndex).resolve("testmod-inputs.h5")));
            PipelineInputsForTest storedInputs = new PipelineInputsForTest(datastoreFileManager,
                Mockito.mock(AlertService.class), pipelineTask);
            hdf5ModuleInterface.readFile(
                taskDirectory.resolve("st-" + subtaskIndex).resolve("testmod-inputs.h5").toFile(),
                storedInputs, false);

            assertTrue(storedInputs.getModelFilenames().contains("foo"));
            assertEquals(1, storedInputs.getModelFilenames().size());

            assertTrue(storedInputs.getDataFilenames()
                .contains("uncalibrated-pixels-" + subtaskIndex + ".science.nc"));
            assertTrue(storedInputs.getDataFilenames()
                .contains("uncalibrated-pixels-" + subtaskIndex + ".collateral.nc"));
            assertTrue(storedInputs.getDataFilenames().contains("everyone-needs-me-0.nc"));
            assertTrue(storedInputs.getDataFilenames().contains("everyone-needs-me-1.nc"));
            assertEquals(4, storedInputs.getDataFilenames().size());

            List<ParametersInterface> pars = storedInputs.getModuleParameters()
                .getModuleParameters();
            if (pars.get(0) instanceof Params1) {
                assertTrue(pars.get(1) instanceof Params2);
            } else {
                assertTrue(pars.get(0) instanceof Params2);
                assertTrue(pars.get(1) instanceof Params1);
            }
            assertEquals(2, pars.size());

            Collection<DataFileType> outputDataFileTypes = PipelineInputsOutputsUtils
                .deserializedOutputFileTypesFromTaskDirectory(taskDirectory);
            assertTrue(outputDataFileTypes.contains(calibratedCollateralPixelDataFileType));
            assertEquals(1, outputDataFileTypes.size());
        }
    }

    /** Tests the subtaskInformation() method. */
    @Test
    public void testSubtaskInformation() {

        Mockito.when(datastoreFileManager.subtaskCount()).thenReturn(7);
        SubtaskInformation subtaskInformation = pipelineInputs.subtaskInformation();
        assertEquals("testmod", subtaskInformation.getModuleName());
        assertEquals("[sector-0002;target;1:1:A]", subtaskInformation.getUowBriefState());
        assertEquals(7, subtaskInformation.getSubtaskCount());

        pipelineInputs.setSingleSubtask(true);
        subtaskInformation = pipelineInputs.subtaskInformation();
        assertEquals("testmod", subtaskInformation.getModuleName());
        assertEquals("[sector-0002;target;1:1:A]", subtaskInformation.getUowBriefState());
        assertEquals(1, subtaskInformation.getSubtaskCount());
    }

    private Map<ClassWrapper<ParametersInterface>, ParameterSet> pipelineParameterSets() {
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parMap = new HashMap<>();
        ClassWrapper<ParametersInterface> c1 = new ClassWrapper<>(Params1.class);
        ParameterSet s1 = new ParameterSet("params1");
        s1.populateFromParametersInstance(new Params1());
        parMap.put(c1, s1);
        return parMap;
    }

    private Map<ClassWrapper<ParametersInterface>, ParameterSet> moduleParameterSets() {
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parMap = new HashMap<>();
        ClassWrapper<ParametersInterface> c2 = new ClassWrapper<>(Params2.class);
        ParameterSet s2 = new ParameterSet("params2");
        s2.populateFromParametersInstance(new Params2());
        parMap.put(c2, s2);
        return parMap;
    }

    public static class Params1 extends Parameters {
        private int dmy1 = 500;
        private double dmy2 = 2856.3;

        public int getDmy1() {
            return dmy1;
        }

        public void setDmy1(int dmy1) {
            this.dmy1 = dmy1;
        }

        public double getDmy2() {
            return dmy2;
        }

        public void setDmy2(double dmy2) {
            this.dmy2 = dmy2;
        }
    }

    public static class Params2 extends Parameters {
        private String dmy3 = "dummy string";
        private boolean[] dmy4 = { true, false };

        public String getDmy3() {
            return dmy3;
        }

        public void setDmy3(String dmy3) {
            this.dmy3 = dmy3;
        }

        public boolean[] getDmy4() {
            return dmy4;
        }

        public void setDmy4(boolean[] dmy4) {
            this.dmy4 = dmy4;
        }
    }

    /**
     * Subclass of {@link DatastoreDirectoryPipelineInputs}. This is necessary because if we create
     * an instance of DatastoreDirectoryPipelineInputs and then apply a Mockito spy to it, the HDF5
     * API fails. Hence we need a subclass that has additional functionality we can use in the
     * places where ordinarily we would use Mockito doReturn() ... when() calls on a spy.
     *
     * @author PT
     */
    public static class PipelineInputsForTest extends DatastoreDirectoryPipelineInputs {

        @ProxyIgnore
        private final AlertService mockedAlertService;

        @ProxyIgnore
        private final DatastoreFileManager mockedDatastoreFileManager;

        @ProxyIgnore
        private boolean singleSubtask;

        public PipelineInputsForTest(DatastoreFileManager datastoreFileManager,
            AlertService alertService, PipelineTask pipelineTask) {
            mockedAlertService = alertService;
            mockedDatastoreFileManager = datastoreFileManager;
            setPipelineTask(pipelineTask);
        }

        @Override
        AlertService alertService() {
            return mockedAlertService;
        }

        @Override
        DatastoreFileManager datastoreFileManager() {
            return mockedDatastoreFileManager;
        }

        @Override
        boolean singleSubtask() {
            return singleSubtask;
        }

        public void setSingleSubtask(boolean singleSubtask) {
            this.singleSubtask = singleSubtask;
        }
    }
}
