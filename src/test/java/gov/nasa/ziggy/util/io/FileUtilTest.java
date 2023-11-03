package gov.nasa.ziggy.util.io;

import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.RESULTS_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Tests the {@link FileUtil} class.
 *
 * @author Bill Wohler
 */
public class FileUtilTest {
    private File testDir;
    private File archiveDir;
    private File testRegularFile;
    private File testSubdir;
    private File testSubdirRegularFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, "/dev/null");

    @Rule
    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR,
        "/path/to/pipeline-results");

    @Before
    public void setUp() throws IOException {
        testDir = directoryRule.directory().toFile();
        testDir.mkdir();
        archiveDir = new File(testDir, "testTar");
        archiveDir.mkdir();
        testRegularFile = new File(testDir, "regular-file.txt");
        FileUtils.touch(testRegularFile);
        testSubdir = new File(testDir, "sub-dir");
        testSubdir.mkdir();
        testSubdirRegularFile = new File(testSubdir, "another-regular-file.txt");
        FileUtils.touch(testSubdirRegularFile);
    }

    @Test
    public void testRegularFilesInDirTree() throws IOException {

        // Get a file to find size of
        Path testDirPath = testDir.toPath();
        Path testSubdirPath = testSubdir.toPath();
        Path codeRoot = Paths
            .get(ZiggyConfiguration.getInstance().getString(PropertyName.WORKING_DIR.property()));
        Path testSrcFile = codeRoot
            .resolve(Paths.get("test", "data", "configuration", "pipeline-definition.xml"));
        Path testFile = testDir.toPath().resolve("pipeline-definition.xml");
        Files.copy(testSrcFile, testFile);

        // Real file in a subdirectory
        new File(testDir, "sub-dir").mkdirs();
        Path subDirTestFile = testSubdirPath.resolve(Paths.get("pipeline-definition.xml"));
        Files.copy(testSrcFile, subDirTestFile);

        // Symlinked file in a real directory
        Files.createSymbolicLink(
            testSubdirPath.resolve(Paths.get("pipeline-definition-symlink.xml")), testSrcFile);

        // Symbolic link to a directory
        Path symlinkSubdir = testDirPath.resolve(Paths.get("symlink-sub-dir"));
        Files.createSymbolicLink(symlinkSubdir, testSubdirPath.toAbsolutePath());

        Map<Path, Path> regularFiles = FileUtil.regularFilesInDirTree(testDirPath);
        assertEquals(8, regularFiles.size());

        // Test that all the mapped files are as expected: top-level directory
        Path valuePath = regularFiles.get(testDirPath.relativize(testFile));
        assertNotNull(valuePath);
        assertEquals(testFile.toAbsolutePath(), valuePath);

        valuePath = regularFiles.get(testDirPath.relativize(testRegularFile.toPath()));
        assertNotNull(valuePath);
        assertEquals(testRegularFile.toPath().toAbsolutePath(), valuePath);

        // sub-dir directory
        valuePath = regularFiles.get(testDirPath.relativize(testSubdirRegularFile.toPath()));
        assertNotNull(valuePath);
        assertEquals(testSubdirRegularFile.toPath().toAbsolutePath(), valuePath);

        valuePath = regularFiles.get(testDirPath.relativize(subDirTestFile));
        assertNotNull(valuePath);
        assertEquals(subDirTestFile.toAbsolutePath(), valuePath);

        valuePath = regularFiles.get(Paths.get("sub-dir", "pipeline-definition-symlink.xml"));
        assertNotNull(valuePath);
        assertEquals(testSrcFile.toAbsolutePath(), valuePath);

        // symlink-sub-dir directory
        valuePath = regularFiles.get(
            testDirPath.relativize(symlinkSubdir.resolve(Paths.get("pipeline-definition.xml"))));
        assertNotNull(valuePath);
        assertEquals(symlinkSubdir.resolve(Paths.get("pipeline-definition.xml")).toAbsolutePath(),
            valuePath);

        valuePath = regularFiles.get(
            testDirPath.relativize(symlinkSubdir.resolve(Paths.get("another-regular-file.txt"))));
        assertNotNull(valuePath);
        assertEquals(symlinkSubdir.resolve(Paths.get("another-regular-file.txt")).toAbsolutePath(),
            valuePath);

        valuePath = regularFiles.get(testDirPath
            .relativize(symlinkSubdir.resolve(Paths.get("pipeline-definition-symlink.xml"))));
        assertNotNull(valuePath);
        assertEquals(testSrcFile.toAbsolutePath(), valuePath);
    }

    @Test
    public void testModeToPosix() {
        assertEquals("r--------", FileUtil.modeToPosixFileString(100));
        assertEquals("-w-------", FileUtil.modeToPosixFileString(200));
        assertEquals("rw-------", FileUtil.modeToPosixFileString(300));
        assertEquals("--x------", FileUtil.modeToPosixFileString(400));
        assertEquals("r-x------", FileUtil.modeToPosixFileString(500));
        assertEquals("-wx------", FileUtil.modeToPosixFileString(600));
        assertEquals("rwx------", FileUtil.modeToPosixFileString(700));

        assertEquals("---r-----", FileUtil.modeToPosixFileString(10));
        assertEquals("------r--", FileUtil.modeToPosixFileString(1));
        assertEquals("---------", FileUtil.modeToPosixFileString(0));

        assertEquals("rwxr----x", FileUtil.modeToPosixFileString(714));
    }

    @Test
    public void testSetPosixPermissionsRecursively() throws IOException {

        // Set permissions restrictively
        assertNotEquals("r-x------",
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir.toPath())));
        FileUtil.setPosixPermissionsRecursively(testDir.toPath(), "r--------", "r-x------");
        assertEquals("r-x------",
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir.toPath())));
        assertEquals("r-x------",
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testSubdir.toPath())));
        assertEquals("r--------",
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testRegularFile.toPath())));
        assertEquals("r--------", PosixFilePermissions
            .toString(Files.getPosixFilePermissions(testSubdirRegularFile.toPath())));
    }

    @Test
    public void testOverwritePermissions() throws IOException {

        // Start by locking everything and checking that at least part of the tree is locked.
        FileUtil.writeProtectDirectoryTree(testDir.toPath());
        assertNotEquals(FileUtil.DIR_OVERWRITE_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir.toPath())));

        FileUtil.prepareDirectoryTreeForOverwrites(testDir.toPath());
        assertEquals(FileUtil.DIR_OVERWRITE_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir.toPath())));
        assertEquals(FileUtil.DIR_OVERWRITE_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testSubdir.toPath())));
        assertEquals(FileUtil.FILE_OVERWRITE_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testRegularFile.toPath())));
        assertEquals(FileUtil.FILE_OVERWRITE_PERMISSIONS, PosixFilePermissions
            .toString(Files.getPosixFilePermissions(testSubdirRegularFile.toPath())));
    }

    @Test
    public void testReadOnlyPermissions() throws IOException {

        // Start by unlocking everything and checking that at least part of the tree is unlocked.
        FileUtil.prepareDirectoryTreeForOverwrites(testDir.toPath());
        assertNotEquals(FileUtil.DIR_READONLY_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir.toPath())));

        FileUtil.writeProtectDirectoryTree(testDir.toPath());
        assertEquals(FileUtil.DIR_READONLY_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir.toPath())));
        assertEquals(FileUtil.DIR_READONLY_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testSubdir.toPath())));
        assertEquals(FileUtil.FILE_READONLY_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testRegularFile.toPath())));
        assertEquals(FileUtil.FILE_READONLY_PERMISSIONS, PosixFilePermissions
            .toString(Files.getPosixFilePermissions(testSubdirRegularFile.toPath())));
    }

    @Test
    public void testCleanDirectoryTree() throws IOException {

        // Create a sub-directory to the test subdir with a regular file in it.
        File testSubSubDir = new File(testSubdir, "sub-sub-dir");
        testSubSubDir.mkdir();
        File subSubDirRegularFile = new File(testSubSubDir, "sub-sub-dir-regular-file.txt");
        FileUtils.touch(subSubDirRegularFile);

        // Add a new directory at the same "level" as testSubdir, with a regular file in it.
        File testNeighborDir = new File(testDir, "neighbor-dir");
        testNeighborDir.mkdir();
        File neighborDirRegularFile = new File(testNeighborDir, "neighbor-regular-file.txt");
        FileUtils.touch(neighborDirRegularFile);

        // Create a symbolic link in the testSubdir to the "neighbor" dir (thus a symlink to
        // a directory with a regular file).
        Path neighborDirLink = testSubdir.toPath().resolve("neighbor-dir-link");
        Files.createSymbolicLink(neighborDirLink, testNeighborDir.toPath());

        // Create a symbolic link to the regular file in the testDir
        Path regularFileLink = testSubdir.toPath().resolve("regular-file-link");
        Files.createSymbolicLink(regularFileLink, testRegularFile.toPath());

        // Write-protect everything inside of testDir
        FileUtil.writeProtectDirectoryTree(testDir.toPath());

        // Clean the testSubdir
        FileUtil.cleanDirectoryTree(testSubdir.toPath(), true);
        assertTrue(testSubdir.exists());
        assertFalse(testSubSubDir.exists());
        assertFalse(neighborDirLink.toFile().exists());
        assertFalse(regularFileLink.toFile().exists());

        // The neighbor directory should still be present and should still have content
        assertTrue(testNeighborDir.exists());
        assertTrue(neighborDirRegularFile.exists());

        // The regular file that was symlinked should still exist
        assertTrue(testRegularFile.exists());

        // Check the file permissions
        assertEquals(FileUtil.DIR_OVERWRITE_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testSubdir.toPath())));
        assertEquals(FileUtil.DIR_READONLY_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testNeighborDir.toPath())));
        assertEquals(FileUtil.FILE_READONLY_PERMISSIONS, PosixFilePermissions
            .toString(Files.getPosixFilePermissions(neighborDirRegularFile.toPath())));
        assertEquals(FileUtil.FILE_READONLY_PERMISSIONS,
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testRegularFile.toPath())));
    }

    @Test(expected = UncheckedIOException.class)
    public void testUnforcedCleanOfWriteProtectedDirTree() throws IOException {
        // Create a sub-directory to the test subdir with a regular file in it.
        File testSubSubDir = new File(testSubdir, "sub-sub-dir");
        testSubSubDir.mkdir();
        File subSubDirRegularFile = new File(testSubSubDir, "sub-sub-dir-regular-file.txt");
        FileUtils.touch(subSubDirRegularFile);

        // Add a new directory at the same "level" as testSubdir, with a regular file in it.
        File testNeighborDir = new File(testDir, "neighbor-dir");
        testNeighborDir.mkdir();
        File neighborDirRegularFile = new File(testNeighborDir, "neighbor-regular-file.txt");
        FileUtils.touch(neighborDirRegularFile);

        // Create a symbolic link in the testSubdir to the "neighbor" dir (thus a symlink to
        // a directory with a regular file).
        Path neighborDirLink = testSubdir.toPath().resolve("neighbor-dir-link");
        Files.createSymbolicLink(neighborDirLink, testNeighborDir.toPath());

        // Create a symbolic link to the regular file in the testDir
        Path regularFileLink = testSubdir.toPath().resolve("regular-file-link");
        Files.createSymbolicLink(regularFileLink, testRegularFile.toPath());

        // Write-protect everything inside of testDir
        FileUtil.writeProtectDirectoryTree(testDir.toPath());

        // Clean the testSubdir
        FileUtil.cleanDirectoryTree(testSubdir.toPath());
    }

    @Test
    public void testDeleteNonexistentDirectory() throws IOException {
        FileUtil.deleteDirectoryTree(testDir.toPath().resolve("non-existent-dir"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteNonDirectoryFile() throws IOException {
        FileUtil.deleteDirectoryTree(testRegularFile.toPath());
    }
}
