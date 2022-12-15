package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.USE_SYMLINKS_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
//import com.google.common.io.Files;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils.DataFileInfoSample1;
import gov.nasa.ziggy.data.management.DataFileTestUtils.DataFileInfoSample2;
import gov.nasa.ziggy.data.management.DataFileTestUtils.DataFileInfoSampleForDirs;
import gov.nasa.ziggy.data.management.DataFileTestUtils.DatastorePathLocatorSample;
import gov.nasa.ziggy.module.AlgorithmStateFiles;
import gov.nasa.ziggy.module.TaskConfigurationManager;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.uow.TaskConfigurationParameters;

/**
 * Test class for DataFileManager class.
 *
 * @author PT
 */
public class DataFileManagerTest {

    private String datastoreRoot = new File(Filenames.BUILD_TEST, "datastore").getAbsolutePath();
    private String taskDirRoot;
    private String taskDir;
    private String subtaskDir;
    private DataFileManager dataFileManager;
    private DataFileManager dataFileManager2;
    private static final long TASK_ID = 100L;
    private static final long PROD_TASK_ID1 = 10L;
    private static final long PROD_TASK_ID2 = 11L;
    private String externalTempDir;

    private PipelineTask pipelineTask;
    private PipelineDefinitionNode pipelineDefinitionNode;
    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud;
    private PipelineTaskCrud pipelineTaskCrud;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, datastoreRoot);

    @Rule
    public ZiggyPropertyRule useSymlinksPropertyRule = new ZiggyPropertyRule(USE_SYMLINKS_PROP_NAME,
        (String) null);

    @Rule
    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, (String) null);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(datastoreRootDirPropertyRule);

    @Before
    public void setup() throws IOException {
        Path datastore = Paths.get(datastoreRootDirPropertyRule.getProperty());
        Files.createDirectories(datastore);
        datastoreRoot = datastore.toString();

        Path taskDirRoot = directoryRule.directory().resolve("taskspace");
        Files.createDirectories(taskDirRoot);
        this.taskDirRoot = taskDirRoot.toString();
        makeTaskDir("pa-5-10");

        Path externalTemp = dirRule.testDirPath().resolve("tmp");
        Files.createDirectories(externalTemp);
        externalTempDir = externalTemp.toAbsolutePath().toString();

        // For some tests we will need a pipeline task and a DatastoreProducerConsumerCrud;
        // set that up now.
        pipelineTask = Mockito.spy(PipelineTask.class);
        datastoreProducerConsumerCrud = new ProducerConsumerCrud();
        pipelineTaskCrud = Mockito.mock(PipelineTaskCrud.class);
        initializeDataFileManager();
        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID);
        pipelineDefinitionNode = Mockito.mock(PipelineDefinitionNode.class);
        Mockito.doReturn(pipelineDefinitionNode).when(pipelineTask).getPipelineDefinitionNode();

        // Now build a DataFileManager for use with DataFileType instances and with the
        // DefaultUnitOfWork.
        initializeDataFileManager2();
        DataFileTestUtils.initializeDataFileTypeSamples();
    }

    @After
    public void teardown() throws InterruptedException, IOException {
        // NB: execution is so fast that some deleteDirectory commands fail because
        // (apparently) write-locks have not yet had time to release! Address this by
        // adding a short nap.
        Thread.sleep(10);
        FileUtils.deleteDirectory(new File(taskDirRoot));
        FileUtil.setPosixPermissionsRecursively(new File(datastoreRoot).toPath(), "rwxrwxrwx");
        FileUtils.forceDelete(new File(Filenames.BUILD_TEST));
    }

    private void makeTaskDir(String taskDirName) {
        File taskDir = new File(taskDirRoot, taskDirName);
        taskDir.mkdirs();
        this.taskDir = taskDir.getAbsolutePath();
        File subtaskDir = new File(taskDir, "st-0");
        subtaskDir.mkdirs();
        this.subtaskDir = subtaskDir.getAbsolutePath();
    }

    /**
     * Tests the dataFilesMap() method of DatastoreFileManager.
     *
     * @throws IOException
     */
    @Test
    public void testDataFilesMap() throws IOException {

        // setup the task directory
        constructTaskDirFiles();

        // construct the set of DatastoreId subclasses
        Set<Class<? extends DataFileInfo>> datastoreIdClasses = new HashSet<>();
        datastoreIdClasses.add(DataFileInfoSample1.class);
        datastoreIdClasses.add(DataFileInfoSample2.class);

        // construct the map
        Map<Class<? extends DataFileInfo>, Set<? extends DataFileInfo>> datastoreIdMap = new DataFileManager()
            .dataFilesMap(Paths.get(taskDir), datastoreIdClasses);

        // The map should have 2 entries
        assertEquals(2, datastoreIdMap.size());

        // The DatastoreIdSample1 entry should have 2 DatastoreIds in it
        @SuppressWarnings("unchecked")
        Set<DataFileInfoSample1> d1Set = (Set<DataFileInfoSample1>) datastoreIdMap
            .get(DataFileInfoSample1.class);
        assertEquals(2, d1Set.size());
        Set<String> names = getNamesFromDatastoreIds(d1Set);
        assertTrue(names.contains("pa-001234567-20-results.h5"));
        assertTrue(names.contains("pa-765432100-20-results.h5"));

        // The DatastoreIdSample2 entry should have 2 DatastoreIds in it
        @SuppressWarnings("unchecked")
        Set<DataFileInfoSample2> d2Set = (Set<DataFileInfoSample2>) datastoreIdMap
            .get(DataFileInfoSample2.class);
        assertEquals(2, d2Set.size());
        names = getNamesFromDatastoreIds(d2Set);
        assertTrue(names.contains("cal-1-1-A-20-results.h5"));
        assertTrue(names.contains("cal-1-1-B-20-results.h5"));

        // NB: the PDC file was ignored, as it should be.
    }

    /**
     * Tests the taskDirectoryDataFilesMap() method of DataFileManager.
     *
     * @throws IOException
     */
    @Test
    public void testTaskDirectoryDataFilesMap() throws IOException {

        constructTaskDirFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Construct the Map
        Map<DataFileType, Set<Path>> dataFileTypeMap = dataFileManager2
            .taskDirectoryDataFilesMap(dataFileTypes);

        assertEquals(2, dataFileTypeMap.size());

        // Make sure the PA files were correctly identified
        Set<Path> d1Set = dataFileTypeMap.get(DataFileTestUtils.dataFileTypeSample1);
        assertEquals(2, d1Set.size());
        Set<String> d1Names = getNamesFromPaths(d1Set);
        assertTrue(d1Names.contains("pa-001234567-20-results.h5"));
        assertTrue(d1Names.contains("pa-765432100-20-results.h5"));

        // Now check the CAL files
        Set<Path> d2Set = dataFileTypeMap.get(DataFileTestUtils.dataFileTypeSample2);
        assertEquals(2, d1Set.size());
        Set<String> d2Names = getNamesFromPaths(d2Set);
        assertTrue(d2Names.contains("cal-1-1-A-20-results.h5"));
        assertTrue(d2Names.contains("cal-1-1-B-20-results.h5"));

    }

    /**
     * Tests the datastoreDataFilesMap method of DataFileManager.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDatastoreDataFilesMap() throws IOException, InterruptedException {

        constructDatastoreFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Construct the Map
        Map<DataFileType, Set<Path>> dataFileTypeMap = dataFileManager2
            .datastoreDataFilesMap(Paths.get(""), dataFileTypes);

        assertEquals(2, dataFileTypeMap.size());

        // Make sure the PA files were correctly identified
        Set<Path> d1Set = dataFileTypeMap.get(DataFileTestUtils.dataFileTypeSample1);
        assertEquals(2, d1Set.size());
        Set<String> d1Names = getNamesFromPaths(d1Set);
        assertTrue(d1Names.contains("pa-001234567-20-results.h5"));
        assertTrue(d1Names.contains("pa-765432100-20-results.h5"));

        // Now check the CAL files
        Set<Path> d2Set = dataFileTypeMap.get(DataFileTestUtils.dataFileTypeSample2);
        assertEquals(2, d1Set.size());
        Set<String> d2Names = getNamesFromPaths(d2Set);
        assertTrue(d2Names.contains("cal-1-1-A-20-results.h5"));
        assertTrue(d2Names.contains("cal-1-1-B-20-results.h5"));

    }

    /**
     * Tests the datastoreFiles() method.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDatastoreFiles() throws IOException, InterruptedException {

        constructTaskDirFiles();

        // construct the set of DatastoreId subclasses
        Set<Class<? extends DataFileInfo>> datastoreIdClasses = new HashSet<>();
        datastoreIdClasses.add(DataFileInfoSample1.class);
        datastoreIdClasses.add(DataFileInfoSample2.class);
        Set<DataFileInfo> datastoreIds = new DataFileManager().datastoreFiles(Paths.get(taskDir),
            datastoreIdClasses);
        assertEquals(4, datastoreIds.size());
        Set<String> names = getNamesFromDatastoreIds(datastoreIds);
        assertTrue(names.contains("pa-001234567-20-results.h5"));
        assertTrue(names.contains("pa-765432100-20-results.h5"));
        assertTrue(names.contains("cal-1-1-A-20-results.h5"));
        assertTrue(names.contains("cal-1-1-B-20-results.h5"));

    }

    /**
     * Tests the copyToTaskDirectory() methods, for individual files.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCopyFilesToTaskDirectory() throws IOException, InterruptedException {

        // Set up the datastore.
        constructDatastoreFiles();

        // Create the DatastoreId objects.
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        // Perform the copy.
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager.copyToTaskDirectory(datastoreIds);
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(4, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertEquals(3, filenames.size());
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));

        // Check that the copies are real copies, not symlinks.
        for (File file : endFileList) {
            assertFalse(java.nio.file.Files.isSymbolicLink(file.toPath()));
        }

        // check that the originators were set correctly
        Set<Long> producerTaskIds = pipelineTask.getProducerTaskIds();
        assertEquals(2, producerTaskIds.size());
        assertTrue(producerTaskIds.contains(PROD_TASK_ID1));
        assertTrue(producerTaskIds.contains(PROD_TASK_ID2));

    }

    /**
     * Test the copyFilesByNameToWorkingDirectory() method in the case in which the files are
     * actually copied and not just symlinked.
     */
    @Test
    public void testCopyFilesByNameToWorkingDirectory() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        // create the DatastoreId objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        // copy files to the task directory
        dataFileManager.copyToTaskDirectory(datastoreIds);
        File[] endFileList = new File(taskDir).listFiles();
        Set<String> filenames = getNamesFromListFiles(endFileList);

        // Set the working directory
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        endFileList = new File(subtaskDir).listFiles();
        assertEquals(0, endFileList.length);
        dataFileManager.copyFilesByNameFromTaskDirToWorkingDir(filenames);
        endFileList = new File(subtaskDir).listFiles();
        assertEquals(3, endFileList.length);
        filenames = getNamesFromListFiles(endFileList);
        assertEquals(3, filenames.size());
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));

        // Check that the copies are real copies, not symlinks
        for (File file : endFileList) {
            assertFalse(java.nio.file.Files.isSymbolicLink(file.toPath()));
        }
    }

    /**
     * Tests the copyToTaskDirectory() methods, for individual files, in the case in which the
     * method constructs symlinks instead of performing true copy operations.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testSymlinkFilesToTaskDirectory() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager();

        // construct a new file in the external temp directory
        Path externalFile = Paths.get(externalTempDir, "pa-001234569-20-results.h5");
        java.nio.file.Files.createFile(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"), externalFile);

        // create the DatastoreId objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        // create the DatastoreId object
        DataFileInfoSample1 pa1 = new DataFileInfoSample1("pa-001234569-20-results.h5");
        datastoreIds.add(pa1);

        // perform the copy
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager.copyToTaskDirectory(datastoreIds);
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(5, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("pa-001234569-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));

        // Check that the copies are actually symlinks
        for (File file : endFileList) {
            if (!file.getName().equals("st-0")) {
                assertTrue(java.nio.file.Files.isSymbolicLink(file.toPath()));
            }
        }

        // check that the copies are symlinks of the correct files
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234567-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-001234567-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-765432100-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-765432100-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-A-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "cal-1-1-A-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-001234569-20-results.h5")));

        // check that the originators were set correctly
        Set<Long> producerTaskIds = pipelineTask.getProducerTaskIds();
        assertEquals(2, producerTaskIds.size());
        assertTrue(producerTaskIds.contains(PROD_TASK_ID1));
        assertTrue(producerTaskIds.contains(PROD_TASK_ID2));
    }

    /**
     * Tests that the search along a symlink path for the true source file doesn't go past the
     * boundaries of the datastore.
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testSymlinkFilesWithSearchLimits() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager();

        // construct a new file in the external temp directory
        Path externalFile = Paths.get(externalTempDir, "pa-001234569-20-results.h5");
        java.nio.file.Files.createFile(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"), externalFile);

        // create the DatastoreId object
        DataFileInfoSample1 pa1 = new DataFileInfoSample1("pa-001234569-20-results.h5");
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(pa1);

        // perform the copy
        dataFileManager.copyToTaskDirectory(datastoreIds);
        File[] endFileList = new File(taskDir).listFiles();
        assertEquals(2, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234569-20-results.h5"));

        // Check that the file is a symlink, and that it's a symlink of the correct source file
        for (File file : endFileList) {
            if (!file.getName().equals("st-0")) {
                assertTrue(java.nio.file.Files.isSymbolicLink(file.toPath()));
            }
        }
        // check that the copies are symlinks of the correct files
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-001234569-20-results.h5")));

    }

    /**
     * Tests that the copyFilesByNameToWorkingDirectory method properly makes symlinks rather than
     * copies.
     */
    @Test
    public void testSymlinkFilesByNameToWorkingDirectory()
        throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager();

        // create the DatastoreId objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        // construct a new file in the external temp directory
        Path externalFile = Paths.get(externalTempDir, "pa-001234569-20-results.h5");
        java.nio.file.Files.createFile(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"), externalFile);

        // create the DatastoreId object
        DataFileInfoSample1 pa1 = new DataFileInfoSample1("pa-001234569-20-results.h5");
        datastoreIds.add(pa1);

        // copy files to the task directory
        dataFileManager.copyToTaskDirectory(datastoreIds);
        File[] endFileList = new File(taskDir).listFiles();
        Set<String> filenames = getNamesFromListFiles(endFileList);

        // Set the working directory
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        endFileList = new File(subtaskDir).listFiles();
        assertEquals(0, endFileList.length);
        dataFileManager.copyFilesByNameFromTaskDirToWorkingDir(filenames);
        endFileList = new File(subtaskDir).listFiles();
        assertEquals(4, endFileList.length);
        filenames = getNamesFromListFiles(endFileList);
        assertEquals(4, filenames.size());
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("pa-001234569-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));

        // Check that the copies are real copies, not symlinks
        for (File file : endFileList) {
            assertTrue(java.nio.file.Files.isSymbolicLink(file.toPath()));
        }

        // check that the copies are symlinks of the correct files (i.e., to the files in the
        // datastore and not the files in the task directory)
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234567-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-001234567-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-765432100-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-765432100-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-001234569-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-A-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(subtaskDir, "cal-1-1-A-20-results.h5")));

    }

    @Test
    public void testSymlinkDirectoriesByNameToWorkingDirectory() throws IOException {

        constructDatastoreDirectories();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager2();

        // construct a new file in the external temp directory and a symlink to same in
        // the datastore
        Path externalFile = Paths.get(externalTempDir, "EO1H0230412000337112N0_WGS_01");
        java.nio.file.Files.createDirectory(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "EO1H0230412000337112N0_WGS_01"), externalFile);

        // Construct the DataFileInfo instances
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112N0_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112NO_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H2240632000337112NP_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230412000337112N0_WGS_01"));

        DataFileTestUtils.initializeDataFileTypeForDirectories();
        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeForDirectories);

        // Copy the files to the task directory
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, null);

        // Now copy the files by name to the subtask directory
        List<String> dirNamesToCopy = new ArrayList<>();
        dirNamesToCopy.add("EO1H0230312000337112N0_WGS_01");
        dirNamesToCopy.add("EO1H0230312000337112NO_WGS_01");
        dirNamesToCopy.add("EO1H2240632000337112NP_WGS_01");
        dirNamesToCopy.add("EO1H0230412000337112N0_WGS_01");

        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        dataFileManager2.copyFilesByNameFromTaskDirToWorkingDir(dirNamesToCopy);

        File[] endFileList = new File(subtaskDir).listFiles();
        assertEquals(4, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList, true);
        assertEquals(4, filenames.size());
        assertTrue(filenames.containsAll(dirNamesToCopy));

        // Check that the symlinks are links of the expected files
        assertEquals(Paths.get(datastoreRoot, "EO1H0230312000337112N0_WGS_01"), java.nio.file.Files
            .readSymbolicLink(Paths.get(subtaskDir, "EO1H0230312000337112N0_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H0230312000337112NO_WGS_01"), java.nio.file.Files
            .readSymbolicLink(Paths.get(subtaskDir, "EO1H0230312000337112NO_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H2240632000337112NP_WGS_01"), java.nio.file.Files
            .readSymbolicLink(Paths.get(subtaskDir, "EO1H2240632000337112NP_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H0230412000337112N0_WGS_01"), java.nio.file.Files
            .readSymbolicLink(Paths.get(subtaskDir, "EO1H0230412000337112N0_WGS_01")));

    }

    /**
     * Tests the deleteFromTaskDirectory() method, for individual files.
     */
    @Test
    public void testDeleteFilesFromTaskDirectory() throws IOException {

        Set<String> datastoreFilenames = constructTaskDirFiles();

        // create the DataFileInfo objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        // Copy the data file objects to the subtask directory
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        dataFileManager.copyFilesByNameFromTaskDirToWorkingDir(datastoreFilenames);

        // The files in the datastoreIds should be gone from the task directory but still
        // present in the subtask directory
        dataFileManager.deleteFromTaskDirectory(datastoreIds);
        Set<String> filesInTaskDir = getNamesFromListFiles(new File(taskDir).listFiles());
        assertEquals(2, filesInTaskDir.size());
        assertTrue(filesInTaskDir.contains("cal-1-1-B-20-results.h5"));
        assertTrue(filesInTaskDir.contains("pdc-1-1-20-results.h5"));
        Set<String> filesInSubtaskDir = getNamesFromListFiles(new File(subtaskDir).listFiles());
        assertEquals(5, filesInSubtaskDir.size());
        for (String filename : datastoreFilenames) {
            assertTrue(filesInSubtaskDir.contains(filename));
            assertFalse(java.nio.file.Files.isSymbolicLink(Paths.get(subtaskDir, filename)));
        }
    }

    /**
     * Tests that when symlinks are deleted from the task directory, they remain in the subtask
     * directory and point to the datastore, not the task directory.
     */
    @Test
    public void testDeleteSymlinksFromTaskDirectory() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager();

        // create the DatastoreId objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        // copy files to the task directory
        dataFileManager.copyToTaskDirectory(datastoreIds);
        File[] endFileList = new File(taskDir).listFiles();
        Set<String> filenames = getNamesFromListFiles(endFileList);

        // Set the working directory
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        dataFileManager.copyFilesByNameFromTaskDirToWorkingDir(filenames);

        // The files in the datastoreIds should be gone from the task directory but still
        // present in the subtask directory
        dataFileManager.deleteFromTaskDirectory(datastoreIds);
        Set<String> filesInTaskDir = getNamesFromListFiles(new File(taskDir).listFiles());
        assertEquals(0, filesInTaskDir.size());
        Set<String> filesInSubtaskDir = getNamesFromListFiles(new File(subtaskDir).listFiles());
        assertEquals(3, filesInSubtaskDir.size());
        assertTrue(filesInSubtaskDir.contains("pa-001234567-20-results.h5"));
        assertTrue(filesInSubtaskDir.contains("pa-765432100-20-results.h5"));
        assertTrue(filesInSubtaskDir.contains("cal-1-1-A-20-results.h5"));

        // The files in the working directory should be symlinks back to the datastore
        Path datastorePath = Paths.get(datastoreRoot);
        for (String filename : filesInSubtaskDir) {
            Path subtaskPath = Paths.get(subtaskDir, filename);
            assertTrue(java.nio.file.Files.isSymbolicLink(subtaskPath));
            assertTrue(java.nio.file.Files.readSymbolicLink(subtaskPath).startsWith(datastorePath));
        }

    }

    /**
     * Tests the moveToDatastore() method, for individual files.
     *
     * @throws IOException
     */
    @Test
    public void testMoveFilesToDatastore() throws IOException {

        constructTaskDirFiles();

        // create the DataFileInfo objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        File paDatastoreFile = new File(datastoreRoot, "pa/20");
        File calDatastoreFile = new File(datastoreRoot, "cal/20");
        File taskDirFile = new File(taskDir);
        paDatastoreFile.mkdirs();
        calDatastoreFile.mkdirs();
        assertEquals(0, paDatastoreFile.listFiles().length);
        File[] taskFileList = taskDirFile.listFiles();
        assertEquals(6, taskFileList.length);
        Set<String> filenames = getNamesFromListFiles(taskFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(filenames.contains("pdc-1-1-20-results.h5"));
        dataFileManager.moveToDatastore(datastoreIds);
        File[] endFileList = paDatastoreFile.listFiles();
        assertEquals(2, endFileList.length);
        filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(checkFilePermissions(endFileList, "r--r--r--"));
        assertTrue(checkForSymlinks(endFileList, false));
        endFileList = calDatastoreFile.listFiles();
        assertEquals(1, endFileList.length);
        assertEquals("cal-1-1-A-20-results.h5", endFileList[0].getName());
        assertTrue(checkFilePermissions(endFileList, "r--r--r--"));
        assertTrue(checkForSymlinks(endFileList, false));
        taskFileList = taskDirFile.listFiles();
        assertEquals(3, taskFileList.length);
        filenames = getNamesFromListFiles(taskFileList);
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(filenames.contains("pdc-1-1-20-results.h5"));

    }

    /**
     * Tests that even when the copy mode for task dir files is symlinks, the move of files from the
     * task dir to the datastore results in actual files, not symlinks.
     *
     * @throws IOException
     */
    @Test
    public void testMoveFilesToDatastoreSymlinkMode() throws IOException {

        constructTaskDirFiles();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager();

        // create the DataFileInfo objects
        Set<DataFileInfo> datastoreIds = constructDatastoreIds();

        File paDatastoreFile = new File(datastoreRoot, "pa/20");
        File calDatastoreFile = new File(datastoreRoot, "cal/20");
        File taskDirFile = new File(taskDir);
        paDatastoreFile.mkdirs();
        calDatastoreFile.mkdirs();
        assertEquals(0, paDatastoreFile.listFiles().length);
        File[] taskFileList = taskDirFile.listFiles();
        assertEquals(6, taskFileList.length);
        Set<String> filenames = getNamesFromListFiles(taskFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(filenames.contains("pdc-1-1-20-results.h5"));
        dataFileManager.moveToDatastore(datastoreIds);
        File[] endFileList = paDatastoreFile.listFiles();
        assertEquals(2, endFileList.length);
        filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(checkFilePermissions(endFileList, "r--r--r--"));
        assertTrue(checkForSymlinks(endFileList, false));
        endFileList = calDatastoreFile.listFiles();
        assertEquals(1, endFileList.length);
        assertEquals("cal-1-1-A-20-results.h5", endFileList[0].getName());
        assertTrue(checkFilePermissions(endFileList, "r--r--r--"));
        assertTrue(checkForSymlinks(endFileList, false));
        taskFileList = taskDirFile.listFiles();
        assertEquals(3, taskFileList.length);
        filenames = getNamesFromListFiles(taskFileList);
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(filenames.contains("pdc-1-1-20-results.h5"));

    }

    /**
     * Tests the copyToTaskDirectory() method when the objects to be copied are themselves
     * directories.
     */
    @Test
    public void testCopyDirectoriesToTaskDirectory() throws IOException {

        // set up the directories in the datastore
        constructDatastoreDirectories();

        // Construct the DataFileInfo instances
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112N0_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112NO_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H2240632000337112NP_WGS_01"));

        // Perform the copy
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager.copyToTaskDirectory(datastoreIds);

        // check the existence of the copied directories
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(4, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList, true);
        assertTrue(filenames.contains("EO1H0230312000337112N0_WGS_01"));
        assertTrue(filenames.contains("EO1H0230312000337112NO_WGS_01"));
        assertTrue(filenames.contains("EO1H2240632000337112NP_WGS_01"));
        assertTrue(filenames.contains("st-0"));

        // check that the copied things are, in fact, directories
        assertTrue(java.nio.file.Files
            .isDirectory(taskDirFile.toPath().resolve("EO1H0230312000337112N0_WGS_01")));
        assertTrue(java.nio.file.Files
            .isDirectory(taskDirFile.toPath().resolve("EO1H0230312000337112NO_WGS_01")));
        assertTrue(java.nio.file.Files
            .isDirectory(taskDirFile.toPath().resolve("EO1H2240632000337112NP_WGS_01")));

        // check that the first directory has the intended content
        File[] dir1FileList = new File(taskDirFile, "EO1H0230312000337112N0_WGS_01").listFiles();
        assertEquals(3, dir1FileList.length);
        filenames = getNamesFromListFiles(dir1FileList, true);
        assertTrue(filenames.contains("EO12000337_00CA00C9_r1_WGS_01.L0"));
        assertTrue(filenames.contains("EO12000337_00CD00CC_r1_WGS_01.L0"));
        assertTrue(filenames.contains("EO12000337_00CF00CE_r1_WGS_01.L0"));
        for (File f : dir1FileList) {
            assertTrue(f.isFile());
        }

        // check that the third directory is empty
        File[] dir3FileList = new File(taskDirFile, "EO1H2240632000337112NP_WGS_01").listFiles();
        assertEquals(0, dir3FileList.length);

        // check that the 2nd directory contains a subdirectory
        File[] dir2FileList = new File(taskDirFile, "EO1H0230312000337112NO_WGS_01").listFiles();
        assertEquals(1, dir2FileList.length);
        assertTrue(dir2FileList[0].isDirectory());
        assertEquals("next-level-down-subdir", dir2FileList[0].getName());

        // get the contents of the subdir and check them
        File[] dir2SubDirList = dir2FileList[0].listFiles();
        assertEquals(1, dir2SubDirList.length);
        assertEquals("next-level-down-content.L0", dir2SubDirList[0].getName());
        assertTrue(dir2SubDirList[0].isFile());

    }

    @Test
    public void testSymlinkDirectoriesToTaskDirectory() throws IOException {

        // set up the directories in the datastore
        constructDatastoreDirectories();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager();

        // construct a new file in the external temp directory and a symlink to same in
        // the datastore
        Path externalFile = Paths.get(externalTempDir, "EO1H0230412000337112N0_WGS_01");
        java.nio.file.Files.createDirectory(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "EO1H0230412000337112N0_WGS_01"), externalFile);

        // Construct the DataFileInfo instances
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112N0_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112NO_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H2240632000337112NP_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230412000337112N0_WGS_01"));

        // Perform the copy
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager.copyToTaskDirectory(datastoreIds);

        // check the existence of the copied directories
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(5, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList, true);
        assertTrue(filenames.contains("EO1H0230312000337112N0_WGS_01"));
        assertTrue(filenames.contains("EO1H0230312000337112NO_WGS_01"));
        assertTrue(filenames.contains("EO1H2240632000337112NP_WGS_01"));
        assertTrue(filenames.contains("EO1H0230412000337112N0_WGS_01"));
        assertTrue(filenames.contains("st-0"));

        // check that the copied things are, in fact, symlinks
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112N0_WGS_01")));
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112NO_WGS_01")));
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H2240632000337112NP_WGS_01")));
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H0230412000337112N0_WGS_01")));

        // Check that the symlinks are links of the expected files
        assertEquals(Paths.get(datastoreRoot, "EO1H0230312000337112N0_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112N0_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H0230312000337112NO_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112NO_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H2240632000337112NP_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H2240632000337112NP_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H0230412000337112N0_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H0230412000337112N0_WGS_01")));

    }

    /**
     * Tests the moveToDatastore() method when the objects to be copied are themselves directories.
     */
    @Test
    public void testMoveDirectoriesToDatastore() throws IOException {

        // set up sub-directories in the task directory
        constructTaskDirSubDirectories();

        // Construct the DataFileInfo instances
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112N0_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112NO_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H2240632000337112NP_WGS_01"));

        // Copy the task dir sub-directories to the datastore
        dataFileManager.moveToDatastore(datastoreIds);

        // check the existence of the copied directories
        File[] endFileList = new File(datastoreRoot).listFiles();
        assertEquals(3, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList, true);
        assertTrue(filenames.contains("EO1H0230312000337112N0_WGS_01"));
        assertTrue(filenames.contains("EO1H0230312000337112NO_WGS_01"));
        assertTrue(filenames.contains("EO1H2240632000337112NP_WGS_01"));
        assertTrue(checkFilePermissions(endFileList, "r-xr-xr-x"));

        Path datastorePath = Paths.get(datastoreRoot);
        // check that the copied things are, in fact, directories
        assertTrue(java.nio.file.Files
            .isDirectory(datastorePath.resolve("EO1H0230312000337112N0_WGS_01")));
        assertTrue(java.nio.file.Files
            .isDirectory(datastorePath.resolve("EO1H0230312000337112NO_WGS_01")));
        assertTrue(java.nio.file.Files
            .isDirectory(datastorePath.resolve("EO1H2240632000337112NP_WGS_01")));

        // check that the first directory has the intended content
        File[] dir1FileList = new File(datastorePath.toFile(), "EO1H0230312000337112N0_WGS_01")
            .listFiles();
        assertEquals(3, dir1FileList.length);
        filenames = getNamesFromListFiles(dir1FileList, true);
        assertTrue(filenames.contains("EO12000337_00CA00C9_r1_WGS_01.L0"));
        assertTrue(filenames.contains("EO12000337_00CD00CC_r1_WGS_01.L0"));
        assertTrue(filenames.contains("EO12000337_00CF00CE_r1_WGS_01.L0"));
        for (File f : dir1FileList) {
            assertTrue(f.isFile());
        }

        // check that the third directory is empty
        File[] dir3FileList = new File(datastorePath.toFile(), "EO1H2240632000337112NP_WGS_01")
            .listFiles();
        assertEquals(0, dir3FileList.length);

        // check that the 2nd directory contains a subdirectory
        File[] dir2FileList = new File(datastorePath.toFile(), "EO1H0230312000337112NO_WGS_01")
            .listFiles();
        assertEquals(1, dir2FileList.length);
        assertTrue(dir2FileList[0].isDirectory());
        assertEquals("next-level-down-subdir", dir2FileList[0].getName());

        // get the contents of the subdir and check them
        File[] dir2SubDirList = dir2FileList[0].listFiles();
        assertEquals(1, dir2SubDirList.length);
        assertEquals("next-level-down-content.L0", dir2SubDirList[0].getName());
        assertTrue(dir2SubDirList[0].isFile());

    }

    /**
     * Tests the copyDataFilesByTypeToTaskDirectory() method of DataFileManager.
     */
    @Test
    public void testCopyDataFilesByTypeToTaskDirectory() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // perform the copy
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, null);
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(5, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(checkForSymlinks(fileList, false));

        // check that the originators were set correctly
        Set<Long> producerTaskIds = pipelineTask.getProducerTaskIds();
        assertEquals(2, producerTaskIds.size());
        assertTrue(producerTaskIds.contains(PROD_TASK_ID1));
        assertTrue(producerTaskIds.contains(PROD_TASK_ID2));

    }

    @Test
    public void testDataFilesForInputsByType() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        TaskConfigurationParameters taskConfig = new TaskConfigurationParameters();
        taskConfig.setReprocess(true);

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Get all the files (reprocessing use-case)
        Set<Path> paths = dataFileManager2.dataFilesForInputs(Paths.get(""), dataFileTypes,
            taskConfig);
        assertEquals(4, paths.size());
        Set<String> filenames = paths.stream()
            .map(s -> s.getFileName().toString())
            .collect(Collectors.toSet());
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));

        // Now get only the ones that are appropriate for reprocessing
        taskConfig.setReprocess(false);
        Mockito
            .when(pipelineTaskCrud.retrieveIdsForPipelineDefinitionNode(
                ArgumentMatchers.<Set<Long>> any(),
                ArgumentMatchers.any(PipelineDefinitionNode.class)))
            .thenReturn(Lists.newArrayList(11L, 12L));

        paths = dataFileManager2.dataFilesForInputs(Paths.get(""), dataFileTypes, taskConfig);
        assertEquals(2, paths.size());
        filenames = paths.stream().map(s -> s.getFileName().toString()).collect(Collectors.toSet());
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));

    }

    /**
     * Tests the copyDataFilesByTypeToTaskDirectory method in the case in which symlinks are to be
     * employed instead of true copies.
     */
    @Test
    public void testSymlinkDataFilesByTypeToTaskDirectory()
        throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager2();

        // construct a new file in the external temp directory
        Path externalFile = Paths.get(externalTempDir, "pa-001234569-20-results.h5");
        java.nio.file.Files.createFile(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"), externalFile);

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // perform the copy
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, null);
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(6, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-001234569-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(checkForSymlinks(fileList, true));

        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234567-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-001234567-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234569-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-001234569-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-765432100-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-765432100-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-A-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "cal-1-1-A-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-B-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "cal-1-1-B-20-results.h5")));

    }

    @Test
    public void testSymlinkDirectoriesByTypeToTaskDirectory() throws IOException {

        // set up the datastore
        constructDatastoreDirectories();

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager2();

        // construct a new file in the external temp directory and a symlink to same in
        // the datastore
        Path externalFile = Paths.get(externalTempDir, "EO1H0230412000337112N0_WGS_01");
        java.nio.file.Files.createDirectory(externalFile);
        java.nio.file.Files.createSymbolicLink(
            Paths.get(datastoreRoot, "EO1H0230412000337112N0_WGS_01"), externalFile);

        Set<DataFileType> dataFileTypes = new HashSet<>();
        DataFileTestUtils.initializeDataFileTypeForDirectories();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeForDirectories);

        // Perform the copy
        File taskDirFile = new File(taskDir);
        File[] fileList = taskDirFile.listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, null);

        // check the existence of the copied directories
        File[] endFileList = taskDirFile.listFiles();
        assertEquals(5, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList, true);
        assertTrue(filenames.contains("EO1H0230312000337112N0_WGS_01"));
        assertTrue(filenames.contains("EO1H0230312000337112NO_WGS_01"));
        assertTrue(filenames.contains("EO1H2240632000337112NP_WGS_01"));
        assertTrue(filenames.contains("EO1H0230412000337112N0_WGS_01"));
        assertTrue(filenames.contains("st-0"));

        // check that the copied things are, in fact, symlinks
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112N0_WGS_01")));
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112NO_WGS_01")));
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H2240632000337112NP_WGS_01")));
        assertTrue(java.nio.file.Files
            .isSymbolicLink(taskDirFile.toPath().resolve("EO1H0230412000337112N0_WGS_01")));

        // Check that the symlinks are links of the expected files
        assertEquals(Paths.get(datastoreRoot, "EO1H0230312000337112N0_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112N0_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H0230312000337112NO_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H0230312000337112NO_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H2240632000337112NP_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H2240632000337112NP_WGS_01")));
        assertEquals(Paths.get(datastoreRoot, "EO1H0230412000337112N0_WGS_01"), java.nio.file.Files
            .readSymbolicLink(taskDirFile.toPath().resolve("EO1H0230412000337112N0_WGS_01")));

    }

    /**
     * Tests the deleteDataFilesByTypeFromTaskDirectory() method of DataFileManager.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDeleteDataFilesByTypeFromTaskDirectory()
        throws IOException, InterruptedException {

        // set up the datastore
        Set<String> datastoreFilenames = constructDatastoreFiles();

        // setup the data file types
        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Copy the files to the task directory
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, null);
        new File(taskDir, "pdc-1-1-20-results.h5").createNewFile();

        // move to the subtask directory and copy the files to there
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        dataFileManager2.copyFilesByNameFromTaskDirToWorkingDir(datastoreFilenames);

        // delete the files
        dataFileManager2.deleteDataFilesByTypeFromTaskDirectory(dataFileTypes);

        // The PDC file should still be present in the task directory
        File[] listFiles = new File(taskDir).listFiles();
        Set<String> filesInTaskDir = getNamesFromListFiles(listFiles);
        assertEquals(1, filesInTaskDir.size());
        assertTrue(filesInTaskDir.contains("pdc-1-1-20-results.h5"));
        assertTrue(checkForSymlinks(listFiles, false));

        // all 5 files should still be present in the subtask directory
        listFiles = new File(subtaskDir).listFiles();
        filesInTaskDir = getNamesFromListFiles(listFiles);
        assertEquals(5, filesInTaskDir.size());
        assertTrue(filesInTaskDir.contains("pdc-1-1-20-results.h5"));
        assertTrue(filesInTaskDir.contains("pa-001234567-20-results.h5"));
        assertTrue(filesInTaskDir.contains("pa-765432100-20-results.h5"));
        assertTrue(filesInTaskDir.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filesInTaskDir.contains("cal-1-1-B-20-results.h5"));
        assertTrue(checkForSymlinks(listFiles, false));

    }

    @Test
    public void testDeleteSymlinksByTypeFromTaskDirectory()
        throws IOException, InterruptedException {

        // set up the datastore
        Set<String> datastoreFilenames = constructDatastoreFiles();
        datastoreFilenames.remove("pdc-1-1-20-results.h5");

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager2();

        // setup the data file types
        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Copy the files to the task directory
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, null);

        // move to the subtask directory and copy the files to there
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        dataFileManager2.copyFilesByNameFromTaskDirToWorkingDir(datastoreFilenames);

        // delete the files
        dataFileManager2.deleteDataFilesByTypeFromTaskDirectory(dataFileTypes);

        // None of the files should still be present in the task directory
        Set<String> filesInTaskDir = getNamesFromListFiles(new File(taskDir).listFiles());
        assertEquals(0, filesInTaskDir.size());

        // all 5 files should still be present in the subtask directory, as symlinks
        File[] listFiles = new File(subtaskDir).listFiles();
        filesInTaskDir = getNamesFromListFiles(listFiles);
        assertEquals(4, filesInTaskDir.size());
        assertTrue(filesInTaskDir.contains("pa-001234567-20-results.h5"));
        assertTrue(filesInTaskDir.contains("pa-765432100-20-results.h5"));
        assertTrue(filesInTaskDir.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filesInTaskDir.contains("cal-1-1-B-20-results.h5"));
        assertTrue(checkForSymlinks(listFiles, true));

        // The files should be symlinks of the datastore files
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234567-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-001234567-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-765432100-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-765432100-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-A-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(subtaskDir, "cal-1-1-A-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-B-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(subtaskDir, "cal-1-1-B-20-results.h5")));
    }

    /**
     * Tests the moveDataFilesByTypeToDatastore() method of DataFileManager.
     */
    @Test
    public void testMoveDataFilesByTypeToDatastore() throws IOException {

        // set up sub-directories in the task directory
        constructTaskDirFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        File paDatastoreFile = new File(datastoreRoot, "pa/20");
        File calDatastoreFile = new File(datastoreRoot, "cal/20");
        File taskDirFile = new File(taskDir);
        paDatastoreFile.mkdirs();
        calDatastoreFile.mkdirs();
        assertEquals(0, paDatastoreFile.listFiles().length);
        File[] taskFileList = taskDirFile.listFiles();
        assertEquals(6, taskFileList.length);
        Set<String> filenames = getNamesFromListFiles(taskFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(filenames.contains("pdc-1-1-20-results.h5"));

        // perform the move
        dataFileManager2.moveDataFilesByTypeToDatastore(dataFileTypes);

        // check both moved and unmoved files
        File[] endFileList = paDatastoreFile.listFiles();
        assertEquals(2, endFileList.length);
        filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(checkFilePermissions(endFileList, "r--r--r--"));
        endFileList = calDatastoreFile.listFiles();
        assertEquals(2, endFileList.length);
        filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
        assertTrue(checkFilePermissions(endFileList, "r--r--r--"));
        taskFileList = taskDirFile.listFiles();
        assertEquals(2, taskFileList.length);
        filenames = getNamesFromListFiles(taskFileList);
        assertTrue(filenames.contains("pdc-1-1-20-results.h5"));

    }

    /**
     * Tests the deleteFromTaskDirectory() method in the case in which the objects to be deleted are
     * directories, including non-empty directories.
     */
    @Test
    public void testDeleteDirectoriesFromTaskDirectory() throws IOException {

        // set up sub-directories in the task directory
        constructTaskDirSubDirectories();

        // Construct the DataFileInfo instances
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112N0_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H0230312000337112NO_WGS_01"));
        datastoreIds.add(new DataFileInfoSampleForDirs("EO1H2240632000337112NP_WGS_01"));

        // delete the directories
        dataFileManager.deleteFromTaskDirectory(datastoreIds);

        // check that they are really gone
        File[] endFileList = new File(taskDir).listFiles();
        assertEquals(1, endFileList.length);
        assertEquals("st-0", endFileList[0].getName());

    }

    @Test
    public void testMoveSymlinkedFileToDatastore() throws IOException {

        // Enable symlinking
        System.setProperty(USE_SYMLINKS_PROP_NAME, "true");
        initializeDataFileManager2();

        // change to the subtask directory and populate with files
        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);
        new File(subtaskDir, "pa-001234567-20-results.h5").createNewFile();
        new File(subtaskDir, "pa-765432100-20-results.h5").createNewFile();
        new File(subtaskDir, "cal-1-1-A-20-results.h5").createNewFile();
        new File(subtaskDir, "cal-1-1-B-20-results.h5").createNewFile();

        // Set up the data file types
        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Set up the datastore directories
        File paDatastoreFile = new File(datastoreRoot, "pa/20");
        File calDatastoreFile = new File(datastoreRoot, "cal/20");
        paDatastoreFile.mkdirs();
        calDatastoreFile.mkdirs();

        // Copy the files to the task directory
        dataFileManager2.copyDataFilesByTypeFromWorkingDirToTaskDir(dataFileTypes);

        // This should result in 4 files in the task directory, all of which are
        // symlinks of the files in the working directory
        File[] fileList = new File(taskDir).listFiles();
        assertTrue(checkForSymlinks(fileList, true));
        assertEquals(Paths.get(subtaskDir, "pa-001234567-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-001234567-20-results.h5")));
        assertEquals(Paths.get(subtaskDir, "pa-765432100-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "pa-765432100-20-results.h5")));
        assertEquals(Paths.get(subtaskDir, "cal-1-1-A-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "cal-1-1-A-20-results.h5")));
        assertEquals(Paths.get(subtaskDir, "cal-1-1-B-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(taskDir, "cal-1-1-B-20-results.h5")));

        // now copy the files back to the datastore
        dataFileManager2.moveDataFilesByTypeToDatastore(dataFileTypes);

        // None of these files should be present in the task directory anymore
        fileList = new File(taskDir).listFiles();
        assertEquals(1, fileList.length);
        assertEquals("st-0", fileList[0].getName());

        // All of the files should be present in the subtask directory, but they should be symlinks
        fileList = new File(subtaskDir).listFiles();
        assertTrue(checkForSymlinks(fileList, true));

        // The files should be symlinks of the files in the datastore, which should themselves be
        // real files
        File[] paFiles = Paths.get(datastoreRoot, "pa", "20").toFile().listFiles();
        assertTrue(checkForSymlinks(paFiles, false));
        File[] calFiles = Paths.get(datastoreRoot, "cal", "20").toFile().listFiles();
        assertTrue(checkForSymlinks(calFiles, false));

        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-001234567-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-001234567-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "pa", "20", "pa-765432100-20-results.h5"),
            java.nio.file.Files
                .readSymbolicLink(Paths.get(subtaskDir, "pa-765432100-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-A-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(subtaskDir, "cal-1-1-A-20-results.h5")));
        assertEquals(Paths.get(datastoreRoot, "cal", "20", "cal-1-1-B-20-results.h5"),
            java.nio.file.Files.readSymbolicLink(Paths.get(subtaskDir, "cal-1-1-B-20-results.h5")));

    }

    @Test
    public void testDatastoreFilesInCompletedSubtasks() throws IOException {

        // setup the task directory
        constructTaskDirFiles();

        // add a second subtask directory
        File subtaskDir2 = new File(taskDir, "st-1");
        subtaskDir2.mkdirs();

        // move one CAL file and one PA file to each directory
        File pa1 = new File(taskDir, "pa-001234567-20-results.h5");
        File pa2 = new File(taskDir, "pa-765432100-20-results.h5");
        File cal1 = new File(taskDir, "cal-1-1-A-20-results.h5");
        File cal2 = new File(taskDir, "cal-1-1-B-20-results.h5");

        Files.move(pa1.toPath(), new File(subtaskDir, pa1.getName()).toPath());
        Files.move(pa2.toPath(), new File(subtaskDir2, pa2.getName()).toPath());
        Files.move(cal1.toPath(), new File(subtaskDir, cal1.getName()).toPath());
        Files.move(cal2.toPath(), new File(subtaskDir2, cal2.getName()).toPath());

        // mark the first subtask directory as completed
        AlgorithmStateFiles asf = new AlgorithmStateFiles(new File(subtaskDir));
        asf.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
//        AlgorithmResultsState.setHasResults(new File(subtaskDir));

        // Create and persist a TaskConfigurationManager instance
        TaskConfigurationManager tcm = new TaskConfigurationManager(new File(taskDir));
        tcm.addFilesForSubtask(new TreeSet<String>());
        tcm.addFilesForSubtask(new TreeSet<String>());
        tcm.persist();

        // Get the flavors of input files
        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // Get the files from the completed subtasks with results (i.e.,
        // at this point, none of the subtasks meet both conditions)
        Set<String> filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithResults(dataFileTypes);
        assertEquals(0, filesFromCompletedSubtasks.size());

        // Now for completed subtasks without results (the first subtask
        // dir)
        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithoutResults(dataFileTypes);
        assertEquals(2, filesFromCompletedSubtasks.size());
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample1
            .datastoreFileNameFromTaskDirFileName(pa1.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample2
            .datastoreFileNameFromTaskDirFileName(cal1.getName())));

        // Set the first subtask directory to "has results"
        new AlgorithmStateFiles(new File(subtaskDir)).setResultsFlag();

        // The first subtask directory's files should come up when testing for
        // completed subtasks with results; nothing should come up when testing for
        // completed subtasks without results.
        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithResults(dataFileTypes);
        assertEquals(2, filesFromCompletedSubtasks.size());
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample1
            .datastoreFileNameFromTaskDirFileName(pa1.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample2
            .datastoreFileNameFromTaskDirFileName(cal1.getName())));

        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithoutResults(dataFileTypes);
        assertEquals(0, filesFromCompletedSubtasks.size());

        // Now mark the 2nd subtask directory as completed
        asf = new AlgorithmStateFiles(subtaskDir2);
        asf.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);

        // The first directory should have all the files for complete with results
        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithResults(dataFileTypes);
        assertEquals(2, filesFromCompletedSubtasks.size());
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample1
            .datastoreFileNameFromTaskDirFileName(pa1.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample2
            .datastoreFileNameFromTaskDirFileName(cal1.getName())));

        // The second directory should have all the files for complete without results
        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithoutResults(dataFileTypes);
        assertEquals(2, filesFromCompletedSubtasks.size());
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample1
            .datastoreFileNameFromTaskDirFileName(pa2.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample2
            .datastoreFileNameFromTaskDirFileName(cal2.getName())));

        // When the 2nd directory is also set to "has results," both dirs should show up
        // in the search for completed with results...
        new AlgorithmStateFiles(subtaskDir2).setResultsFlag();

        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithResults(dataFileTypes);
        assertEquals(4, filesFromCompletedSubtasks.size());
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample1
            .datastoreFileNameFromTaskDirFileName(pa1.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample2
            .datastoreFileNameFromTaskDirFileName(cal1.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample1
            .datastoreFileNameFromTaskDirFileName(pa2.getName())));
        assertTrue(filesFromCompletedSubtasks.contains(DataFileTestUtils.dataFileTypeSample2
            .datastoreFileNameFromTaskDirFileName(cal2.getName())));

        // The complete without results search should return nothing
        filesFromCompletedSubtasks = dataFileManager2
            .datastoreFilesInCompletedSubtasksWithoutResults(dataFileTypes);
        assertEquals(0, filesFromCompletedSubtasks.size());

    }

    @Test
    public void testFilesInCompletedSubtasks() throws IOException {

        // setup the task directory
        constructTaskDirFiles();

        // add a second subtask directory
        File subtaskDir2 = new File(taskDir, "st-1");
        subtaskDir2.mkdirs();

        // move one CAL file and one PA file to each directory
        File pa1 = new File(taskDir, "pa-001234567-20-results.h5");
        File pa2 = new File(taskDir, "pa-765432100-20-results.h5");
        File cal1 = new File(taskDir, "cal-1-1-A-20-results.h5");
        File cal2 = new File(taskDir, "cal-1-1-B-20-results.h5");

        Files.move(pa1.toPath(), new File(subtaskDir, pa1.getName()).toPath());
        Files.move(pa2.toPath(), new File(subtaskDir2, pa2.getName()).toPath());
        Files.move(cal1.toPath(), new File(subtaskDir, cal1.getName()).toPath());
        Files.move(cal2.toPath(), new File(subtaskDir2, cal2.getName()).toPath());

        // mark the first subtask directory as completed
        AlgorithmStateFiles asf = new AlgorithmStateFiles(new File(subtaskDir));
        asf.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(new File(subtaskDir)).setResultsFlag();

        // Create and persist a TaskConfigurationManager instance
        TaskConfigurationManager tcm = new TaskConfigurationManager(new File(taskDir));
        tcm.addFilesForSubtask(new TreeSet<String>());
        tcm.addFilesForSubtask(new TreeSet<String>());
        tcm.persist();

        // construct the set of DatastoreId subclasses
        Set<Class<? extends DataFileInfo>> datastoreIdClasses = new HashSet<>();
        datastoreIdClasses.add(DataFileInfoSample1.class);
        datastoreIdClasses.add(DataFileInfoSample2.class);

        initializeDataFileManager();
        Set<String> filenames = dataFileManager
            .filesInCompletedSubtasksWithResults(datastoreIdClasses);
        assertEquals(2, filenames.size());
        assertTrue(
            filenames.contains(Paths.get(datastoreRoot, "pa", "20", pa1.getName()).toString()));
        assertTrue(
            filenames.contains(Paths.get(datastoreRoot, "cal", "20", cal1.getName()).toString()));

        // Now mark the second subtask as completed
        asf = new AlgorithmStateFiles(subtaskDir2);
        asf.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
        new AlgorithmStateFiles(subtaskDir2).setResultsFlag();

        filenames = dataFileManager.filesInCompletedSubtasksWithResults(datastoreIdClasses);
        assertEquals(4, filenames.size());
        assertTrue(
            filenames.contains(Paths.get(datastoreRoot, "pa", "20", pa1.getName()).toString()));
        assertTrue(
            filenames.contains(Paths.get(datastoreRoot, "cal", "20", cal1.getName()).toString()));
        assertTrue(
            filenames.contains(Paths.get(datastoreRoot, "pa", "20", pa2.getName()).toString()));
        assertTrue(
            filenames.contains(Paths.get(datastoreRoot, "cal", "20", cal2.getName()).toString()));
    }

    @Test
    public void testStandardReprocessing() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        TaskConfigurationParameters tcp = new TaskConfigurationParameters();
        tcp.setReprocess(true);
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, tcp);
        File[] endFileList = new File(taskDir).listFiles();
        assertEquals(5, endFileList.length);
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertTrue(filenames.contains("pa-001234567-20-results.h5"));
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-A-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));
    }

    @Test
    public void testKeepUpProcessing() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // set up the pipeline task CRUD
        Mockito
            .when(pipelineTaskCrud.retrieveIdsForPipelineDefinitionNode(
                ArgumentMatchers.<Set<Long>> any(),
                ArgumentMatchers.any(PipelineDefinitionNode.class)))
            .thenReturn(Lists.newArrayList(11L, 12L));

        TaskConfigurationParameters tcp = new TaskConfigurationParameters();
        tcp.setReprocess(false);
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, tcp);
        File[] endFileList = new File(taskDir).listFiles();
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertEquals(2, filenames.size());
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));
        assertTrue(filenames.contains("cal-1-1-B-20-results.h5"));

    }

    @Test
    public void testReprocessingWithExcludes() throws IOException, InterruptedException {

        // set up the datastore
        constructDatastoreFiles();

        Set<DataFileType> dataFileTypes = new HashSet<>();
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypes.add(DataFileTestUtils.dataFileTypeSample2);

        // set up the pipeline task CRUD
        Mockito
            .when(pipelineTaskCrud.retrieveIdsForPipelineDefinitionNode(
                ArgumentMatchers.<Set<Long>> any(),
                ArgumentMatchers.any(PipelineDefinitionNode.class)))
            .thenReturn(Lists.newArrayList(10L, 11L, 12L));

        TaskConfigurationParameters tcp = new TaskConfigurationParameters();
        tcp.setReprocess(true);
        tcp.setReprocessingTasksExclude(new long[] { 10L });
        dataFileManager2.copyDataFilesByTypeToTaskDirectory(Paths.get(""), dataFileTypes, tcp);
        File[] endFileList = new File(taskDir).listFiles();
        Set<String> filenames = getNamesFromListFiles(endFileList);
        assertEquals(1, filenames.size());
        assertTrue(filenames.contains("pa-765432100-20-results.h5"));

    }

    @Test
    public void testWorkingDirHasFilesOfTypes() throws IOException {

        // Put PA data files into the subtask directory
        File sample1 = new File(subtaskDir, "pa-001234567-20-results.h5");
        File sample2 = new File(subtaskDir, "pa-765432100-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();

        System.setProperty(ZIGGY_TEST_WORKING_DIR_PROP_NAME, subtaskDir);

        // Files of the type of the DataFileTypeSample1 should be found.
        assertTrue(dataFileManager2.workingDirHasFilesOfTypes(
            Collections.singleton(DataFileTestUtils.dataFileTypeSample1)));

        // Files of the type of the DataFileTypeSample2 should not be found.
        assertFalse(dataFileManager2.workingDirHasFilesOfTypes(
            Collections.singleton(DataFileTestUtils.dataFileTypeSample2)));

        Set<DataFileType> dataFileTypeSet = new HashSet<>();
        dataFileTypeSet.add(DataFileTestUtils.dataFileTypeSample1);
        dataFileTypeSet.add(DataFileTestUtils.dataFileTypeSample2);

        // When searching for both data types, a result of true should be
        // returned.
        assertTrue(dataFileManager2.workingDirHasFilesOfTypes(dataFileTypeSet));

    }

    private static List<DatastoreProducerConsumer> datastoreProducerConsumers() {

        List<DatastoreProducerConsumer> dpcs = new ArrayList<>();

        DatastoreProducerConsumer dpc = new DatastoreProducerConsumer(1L,
            "pa/20/pa-001234567-20-results.h5", DatastoreProducerConsumer.DataReceiptFileType.DATA);
        dpc.setConsumers(Sets.newHashSet(10L, 11L, 12L));
        dpcs.add(dpc);

        dpc = new DatastoreProducerConsumer(1L, "pa/20/pa-765432100-20-results.h5",
            DatastoreProducerConsumer.DataReceiptFileType.DATA);
        dpc.setConsumers(Sets.newHashSet());
        dpcs.add(dpc);

        // Set up the 1-1-A-20 data file such that it ran in task 11 but produced no
        // results. This should prevent it from being included in reprocessing because
        // the pipeline module that's doing the reprocessing is the same as the module
        // for tasks 11 and 12.
        dpc = new DatastoreProducerConsumer(1L, "cal/20/cal-1-1-A-20-results.h5",
            DatastoreProducerConsumer.DataReceiptFileType.DATA);
        dpc.setConsumers(Sets.newHashSet(10L, -11L));
        dpcs.add(dpc);

        dpc = new DatastoreProducerConsumer(1L, "cal/20/cal-1-1-B-20-results.h5",
            DatastoreProducerConsumer.DataReceiptFileType.DATA);
        dpc.setConsumers(Sets.newHashSet(10L));
        dpcs.add(dpc);

        return dpcs;
    }

    private Set<String> constructTaskDirFiles() throws IOException {

        File taskDir = new File(this.taskDir);
        Set<String> filenames = new HashSet<>();
        // create a couple of files in the DatastoreIdSample1 pattern
        File sample1 = new File(taskDir, "pa-001234567-20-results.h5");
        File sample2 = new File(taskDir, "pa-765432100-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        filenames.add(sample1.getName());
        filenames.add(sample2.getName());

        // create a couple of files in the DatastoreIdSample2 pattern
        sample1 = new File(taskDir, "cal-1-1-A-20-results.h5");
        sample2 = new File(taskDir, "cal-1-1-B-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        filenames.add(sample1.getName());
        filenames.add(sample2.getName());

        // create a file that matches neither pattern
        sample1 = new File(taskDir, "pdc-1-1-20-results.h5");
        sample1.createNewFile();
        filenames.add(sample1.getName());
        return filenames;

    }

    private Set<String> constructDatastoreFiles() throws IOException, InterruptedException {

        Set<String> datastoreFilenames = new HashSet<>();

        // create some directories in the datastore
        File paDir = new File(datastoreRoot, "pa/20");
        File calDir = new File(datastoreRoot, "cal/20");
        File pdcDir = new File(datastoreRoot, "pdc/20");
        paDir.mkdirs();
        calDir.mkdirs();
        pdcDir.mkdirs();

        // create the files
        File sample1 = new File(paDir, "pa-001234567-20-results.h5");
        File sample2 = new File(paDir, "pa-765432100-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        datastoreFilenames.add(sample1.getName());
        datastoreFilenames.add(sample2.getName());

        sample1 = new File(calDir, "cal-1-1-A-20-results.h5");
        sample2 = new File(calDir, "cal-1-1-B-20-results.h5");
        sample1.createNewFile();
        sample2.createNewFile();
        datastoreFilenames.add(sample1.getName());
        datastoreFilenames.add(sample2.getName());

        sample1 = new File(pdcDir, "pdc-1-1-20-results.h5");
        sample1.createNewFile();
        boolean dmy = true;
        dmy = !dmy;
        datastoreFilenames.add(sample1.getName());
        return datastoreFilenames;

    }

    private <T extends DataFileInfo> Set<String> getNamesFromDatastoreIds(Set<T> datastoreSet) {
        Set<String> names = new HashSet<>();
        for (T d : datastoreSet) {
            names.add(d.toString());
        }
        return names;
    }

    private Set<String> getNamesFromListFiles(File[] files) {
        return getNamesFromListFiles(files, false);
    }

    private Set<String> getNamesFromListFiles(File[] files, boolean acceptDirs) {
        Set<String> nameSet = new HashSet<>();
        for (File f : files) {
            if (!f.isDirectory() || acceptDirs) {
                nameSet.add(f.getName());
            }
        }
        return nameSet;
    }

    private Set<String> getNamesFromPaths(Set<Path> paths) {
        Set<String> nameSet = new HashSet<>();
        for (Path p : paths) {
            nameSet.add(p.getFileName().toString());
        }
        return nameSet;
    }

    private boolean checkFilePermissions(File[] files, String permissions) throws IOException {
        Set<PosixFilePermission> intendedPermissions = PosixFilePermissions.fromString(permissions);
        boolean permissionsCorrect = true;
        for (File file : files) {
            Set<PosixFilePermission> actualPermissions = java.nio.file.Files
                .getPosixFilePermissions(file.toPath());
            permissionsCorrect = permissionsCorrect
                && actualPermissions.size() == intendedPermissions.size();
            for (PosixFilePermission permission : intendedPermissions) {
                permissionsCorrect = permissionsCorrect && actualPermissions.contains(permission);
            }
        }
        return permissionsCorrect;
    }

    private boolean checkForSymlinks(File[] files, boolean symlinkExpected) {
        boolean allFilesMatchExpected = true;
        for (File file : files) {
            if (!file.isDirectory()) {
                allFilesMatchExpected = allFilesMatchExpected
                    && java.nio.file.Files.isSymbolicLink(file.toPath()) == symlinkExpected;
            }
        }
        return allFilesMatchExpected;
    }

    private void initializeDataFileManager() {
        dataFileManager = new DataFileManager(new DatastorePathLocatorSample(), pipelineTask,
            Paths.get(taskDir));
        dataFileManager = Mockito.spy(dataFileManager);
        Mockito.when(dataFileManager.datastoreProducerConsumerCrud())
            .thenReturn(datastoreProducerConsumerCrud);
        Mockito.when(dataFileManager.pipelineTaskCrud()).thenReturn(pipelineTaskCrud);
    }

    private void initializeDataFileManager2() {
        dataFileManager2 = new DataFileManager(Paths.get(datastoreRoot), Paths.get(taskDir),
            pipelineTask);
        dataFileManager2 = Mockito.spy(dataFileManager2);
        Mockito.when(dataFileManager2.datastoreProducerConsumerCrud())
            .thenReturn(datastoreProducerConsumerCrud);
        Mockito.when(dataFileManager2.pipelineTaskCrud()).thenReturn(pipelineTaskCrud);
    }

    /**
     * Constructs directories in the datastore that follow the Hyperion naming convention, with
     * files inside them.
     *
     * @throws IOException
     */
    private void constructDatastoreDirectories() throws IOException {

        // Create the directories
        File dir1 = new File(datastoreRoot, "EO1H0230312000337112N0_WGS_01");
        File dir2 = new File(datastoreRoot, "EO1H0230312000337112NO_WGS_01");
        File dir3 = new File(datastoreRoot, "EO1H2240632000337112NP_WGS_01");
        dir1.mkdirs();
        dir2.mkdirs();
        dir3.mkdirs();

        // create contents in 2 out of 3 directories
        File sample1 = new File(dir1, "EO12000337_00CA00C9_r1_WGS_01.L0");
        File sample2 = new File(dir1, "EO12000337_00CD00CC_r1_WGS_01.L0");
        File sample3 = new File(dir1, "EO12000337_00CF00CE_r1_WGS_01.L0");
        sample1.createNewFile();
        sample2.createNewFile();
        sample3.createNewFile();

        // In this directory, create a subdirectory as well
        File dir4 = new File(dir2, "next-level-down-subdir");
        dir4.mkdirs();
        File sample4 = new File(dir4, "next-level-down-content.L0");
        sample4.createNewFile();
    }

    private Set<DataFileInfo> constructDatastoreIds() {
        DataFileInfoSample1 pa1 = new DataFileInfoSample1("pa-001234567-20-results.h5");
        DataFileInfoSample1 pa2 = new DataFileInfoSample1("pa-765432100-20-results.h5");
        Set<DataFileInfo> datastoreIds = new HashSet<>();
        datastoreIds.add(pa1);
        datastoreIds.add(pa2);
        DataFileInfoSample2 cal1 = new DataFileInfoSample2("cal-1-1-A-20-results.h5");
        datastoreIds.add(cal1);
        return datastoreIds;
    }

    private void constructTaskDirSubDirectories() throws IOException {

        // Create the directories
        File dir1 = new File(taskDir, "EO1H0230312000337112N0_WGS_01");
        File dir2 = new File(taskDir, "EO1H0230312000337112NO_WGS_01");
        File dir3 = new File(taskDir, "EO1H2240632000337112NP_WGS_01");
        dir1.mkdirs();
        dir2.mkdirs();
        dir3.mkdirs();

        // create contents in 2 out of 3 directories
        File sample1 = new File(dir1, "EO12000337_00CA00C9_r1_WGS_01.L0");
        File sample2 = new File(dir1, "EO12000337_00CD00CC_r1_WGS_01.L0");
        File sample3 = new File(dir1, "EO12000337_00CF00CE_r1_WGS_01.L0");
        sample1.createNewFile();
        sample2.createNewFile();
        sample3.createNewFile();

        // In this directory, create a subdirectory as well
        File dir4 = new File(dir2, "next-level-down-subdir");
        dir4.mkdirs();
        File sample4 = new File(dir4, "next-level-down-content.L0");
        sample4.createNewFile();
    }

    /**
     * Provides a subclass of {@link DatastoreProducerConsumerCrud} for use in the unit tests of
     * different processing modes. Necessary because it was far from obvious how to properly mock
     * the behavior of the class in question via Mockito.
     *
     * @author PT
     */
    private static class ProducerConsumerCrud extends DatastoreProducerConsumerCrud {

        @Override
        public List<DatastoreProducerConsumer> retrieveByFilename(Set<Path> datafiles) {

            List<DatastoreProducerConsumer> returns = new ArrayList<>();
            List<DatastoreProducerConsumer> dpcs = datastoreProducerConsumers();
            for (DatastoreProducerConsumer dpc : dpcs) {
                if (datafiles.contains(Paths.get(dpc.getFilename()))) {
                    returns.add(dpc);
                }
            }
            return returns;
        }

        @Override
        public Set<Long> retrieveProducers(Set<Path> paths) {
            return Sets.newHashSet(PROD_TASK_ID1, PROD_TASK_ID2);
        }

        @Override
        public void createOrUpdateProducer(PipelineTask pipelineTask,
            Collection<Path> datastoreFiles, DatastoreProducerConsumer.DataReceiptFileType type) {

        }
    }

}
