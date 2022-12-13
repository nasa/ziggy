package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.USE_SYMLINKS_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.DirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Unit tests for {@link DefaultDataImporter} class.
 *
 * @author PT
 */
public class DefaultDataImporterTest {

    private PipelineTask pipelineTask = Mockito.mock(PipelineTask.class);
    private Path testDirPath;
    private Path dataImporterPath;
    private Path dataImporterSubdirPath;
    private Path dirForImports;
    private Path datastoreRootPath;
    private UnitOfWork singleUow = new UnitOfWork();
    private UnitOfWork subdirUow = new UnitOfWork();
    private PipelineDefinitionNode node = Mockito.mock(PipelineDefinitionNode.class);

    @Rule
    public ZiggyDirectoryRule dirRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, dirRule.testDirPath().resolve("datastore").toString());

    @Rule
    public ZiggyPropertyRule useSymlinksPropertyRule = new ZiggyPropertyRule(USE_SYMLINKS_PROP_NAME,
        null);

    @Before
    public void setUp() throws IOException {

        // Construct the necessary directories.
        testDirPath = dirRule.testDirPath();
        dataImporterPath = testDirPath.resolve("data-import");
        dataImporterPath.toFile().mkdirs();
        datastoreRootPath = testDirPath.resolve("datastore");
        datastoreRootPath.toFile().mkdirs();
        dataImporterSubdirPath = dataImporterPath.resolve("sub-dir");
        dataImporterSubdirPath.toFile().mkdirs();
        singleUow.addParameter(new TypedParameter(
            UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        subdirUow.addParameter(new TypedParameter(
            UnitOfWorkGenerator.GENERATOR_CLASS_PARAMETER_NAME,
            DataReceiptUnitOfWorkGenerator.class.getCanonicalName(), ZiggyDataType.ZIGGY_STRING));
        subdirUow
            .addParameter(new TypedParameter(DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME,
                "sub-dir", ZiggyDataType.ZIGGY_STRING));
        singleUow.addParameter(new TypedParameter(
            DirectoryUnitOfWorkGenerator.DIRECTORY_PROPERTY_NAME, "", ZiggyDataType.ZIGGY_STRING));

        // Initialize the data type samples
        DataFileTestUtils.initializeDataFileTypeSamples();

        // construct the files for import
        constructFilesForImport();

        // Construct the data file type information
        Mockito.when(node.getInputDataFileTypes())
            .thenReturn(ImmutableSet.of(DataFileTestUtils.dataFileTypeSample1,
                DataFileTestUtils.dataFileTypeSample2));
    }

    @Test
    public void testSingleUowConstructor() {
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(singleUow));
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        assertEquals(dataImporterPath, importer.getDataImportPath());
    }

    @Test
    public void testDefaultUowConstructor() {
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(subdirUow));
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        assertEquals(dataImporterSubdirPath, importer.getDataImportPath());
    }

    @Test
    public void testDataFilesInMainDir() throws IOException {
        dirForImports = dataImporterPath;
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(singleUow));
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        Map<Path, Path> dataFiles = importer.dataFiles(filenamesInDirectory());
        assertEquals(2, dataFiles.size());
        assertTrue(dataFiles.containsKey(Paths.get("pa-001234567-20-results.h5")));
        assertEquals(dataFiles.get(Paths.get("pa-001234567-20-results.h5")),
            Paths.get("pa", "20", "pa-001234567-20-results.h5"));
        assertTrue(dataFiles.containsKey(Paths.get("cal-1-1-A-20-results.h5")));
        assertEquals(dataFiles.get(Paths.get("cal-1-1-A-20-results.h5")),
            Paths.get("cal", "20", "cal-1-1-A-20-results.h5"));
    }

    @Test
    public void testDataFilesInSubdir() throws IOException {
        dirForImports = dataImporterSubdirPath;
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(subdirUow));
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        Map<Path, Path> dataFiles = importer.dataFiles(filenamesInDirectory());
        assertEquals(2, dataFiles.size());
        assertTrue(dataFiles.containsKey(Paths.get("pa-765432100-20-results.h5")));
        assertEquals(dataFiles.get(Paths.get("pa-765432100-20-results.h5")),
            Paths.get("pa", "20", "pa-765432100-20-results.h5"));
        assertTrue(dataFiles.containsKey(Paths.get("cal-1-1-B-20-results.h5")));
        assertEquals(dataFiles.get(Paths.get("cal-1-1-B-20-results.h5")),
            Paths.get("cal", "20", "cal-1-1-B-20-results.h5"));
    }

    @Test
    public void testImportFilesFromMainDir() throws IOException {
        dirForImports = dataImporterPath;
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(singleUow));
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        Map<Path, Path> dataFiles = importer.dataFiles(filenamesInDirectory());
        Set<Path> importedFiles = importer.importFiles(dataFiles);
        assertEquals(2, importedFiles.size());
        assertTrue(importedFiles.contains(Paths.get("pa-001234567-20-results.h5")));
        assertTrue(importedFiles.contains(Paths.get("cal-1-1-A-20-results.h5")));

        // The files should be in the correct locations in the datastore
        assertTrue(datastoreRootPath.resolve(Paths.get("pa", "20", "pa-001234567-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve(Paths.get("cal", "20", "cal-1-1-A-20-results.h5"))
            .toFile()
            .exists());

        // The PDC file should remain in the import directory
        assertEquals(2, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve(Paths.get("sub-dir")).toFile().exists());
        assertTrue(dataImporterPath.resolve(Paths.get("pdc-1-1-20-results.h5")).toFile().exists());

        // The subdir files should be untouched
        assertEquals(2, dataImporterSubdirPath.toFile().listFiles().length);
        assertTrue(dataImporterSubdirPath.resolve(Paths.get("pa-765432100-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(
            dataImporterSubdirPath.resolve(Paths.get("cal-1-1-B-20-results.h5")).toFile().exists());
    }

    @Test
    public void testImportFilesFromSubdir() throws IOException {
        dirForImports = dataImporterSubdirPath;
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(subdirUow));
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        Map<Path, Path> dataFiles = importer.dataFiles(filenamesInDirectory());
        Set<Path> importedFiles = importer.importFiles(dataFiles);
        assertEquals(2, importedFiles.size());
        assertTrue(importedFiles.contains(Paths.get("pa-765432100-20-results.h5")));
        assertTrue(importedFiles.contains(Paths.get("cal-1-1-B-20-results.h5")));

        // The files should be in the correct locations in the datastore
        assertTrue(datastoreRootPath.resolve(Paths.get("pa", "20", "pa-765432100-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(datastoreRootPath.resolve(Paths.get("cal", "20", "cal-1-1-B-20-results.h5"))
            .toFile()
            .exists());

        // The top-level import directory should be untouched
        assertEquals(4, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve(Paths.get("sub-dir")).toFile().exists());
        assertTrue(dataImporterPath.resolve(Paths.get("pdc-1-1-20-results.h5")).toFile().exists());
        assertTrue(
            dataImporterPath.resolve(Paths.get("pa-001234567-20-results.h5")).toFile().exists());
        assertTrue(
            dataImporterPath.resolve(Paths.get("cal-1-1-A-20-results.h5")).toFile().exists());

        // The subdir should be empty
        assertEquals(0, dataImporterSubdirPath.toFile().listFiles().length);
    }

    @Test
    public void testImportFilesWithFailure() throws IOException {
        dirForImports = dataImporterPath;
        Mockito.when(pipelineTask.getUowTask()).thenReturn(new BeanWrapper<>(singleUow));
        Mockito.when(pipelineTask.getPipelineDefinitionNode()).thenReturn(node);
        DefaultDataImporter importer = new DefaultDataImporter(pipelineTask, dataImporterPath,
            datastoreRootPath);
        importer = Mockito.spy(importer);
        Mockito.doThrow(IOException.class)
            .when(importer)
            .moveOrSymlink(dataImporterPath.resolve(Paths.get("pa-001234567-20-results.h5")),
                datastoreRootPath.resolve(Paths.get("pa", "20", "pa-001234567-20-results.h5")));
        Map<Path, Path> dataFiles = importer.dataFiles(filenamesInDirectory());
        Set<Path> importedFiles = importer.importFiles(dataFiles);
        assertEquals(1, importedFiles.size());
        assertTrue(importedFiles.contains(Paths.get("cal-1-1-A-20-results.h5")));

        // The file should be in the correct locations in the datastore
        assertTrue(datastoreRootPath.resolve(Paths.get("cal", "20", "cal-1-1-A-20-results.h5"))
            .toFile()
            .exists());

        // The PDC and PA files should remain in the import directory
        assertEquals(3, dataImporterPath.toFile().listFiles().length);
        assertTrue(dataImporterPath.resolve(Paths.get("sub-dir")).toFile().exists());
        assertTrue(dataImporterPath.resolve(Paths.get("pdc-1-1-20-results.h5")).toFile().exists());
        assertTrue(
            dataImporterPath.resolve(Paths.get("pa-001234567-20-results.h5")).toFile().exists());

        // The subdir files should be untouched
        assertEquals(2, dataImporterSubdirPath.toFile().listFiles().length);
        assertTrue(dataImporterSubdirPath.resolve(Paths.get("pa-765432100-20-results.h5"))
            .toFile()
            .exists());
        assertTrue(
            dataImporterSubdirPath.resolve(Paths.get("cal-1-1-B-20-results.h5")).toFile().exists());
    }

    /**
     * Creates test files for import in the data receipt directory
     */
    private Set<String> constructFilesForImport() throws IOException {

        Set<String> filenames = new HashSet<>();
        // create a couple of files in the DatastoreIdSample1 pattern
        File sample1 = new File(dataImporterPath.toFile(), "pa-001234567-20-results.h5");
        File sample2 = new File(dataImporterSubdirPath.toFile(), "pa-765432100-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        filenames.add(sample1.getName());
        filenames.add(sample2.getName());

        // create a couple of files in the DatastoreIdSample2 pattern
        sample1 = new File(dataImporterPath.toFile(), "cal-1-1-A-20-results.h5");
        sample2 = new File(dataImporterSubdirPath.toFile(), "cal-1-1-B-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        filenames.add(sample1.getName());
        filenames.add(sample2.getName());

        // create a file that matches neither pattern
        sample1 = new File(dataImporterPath.toFile(), "pdc-1-1-20-results.h5");
        sample1.createNewFile();
        filenames.add(sample1.getName());
        return filenames;

    }

    private List<String> filenamesInDirectory() throws IOException {
        List<String> filenamesInDirectory = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirForImports)) {
            for (Path path : stream) {
                filenamesInDirectory.add(path.getFileName().toString());
            }
        }
        return filenamesInDirectory;
    }

}
