package gov.nasa.ziggy.util.io;

import static gov.nasa.ziggy.services.config.PropertyNames.DATASTORE_ROOT_DIR_PROP_NAME;
import static gov.nasa.ziggy.services.config.PropertyNames.RESULTS_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;

/**
 * Tests the {@link FileUtil} class.
 *
 * @author Bill Wohler
 */
public class FileUtilTest {
    private Path testDir;
    private Path archiveDir;
    private Path testRegularFile;
    private Path testSubdir;
    private Path testSubdirRegularFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR_PROP_NAME, "/dev/null");

    @Rule
    public ZiggyPropertyRule resultsDirPropertyRule = new ZiggyPropertyRule(RESULTS_DIR_PROP_NAME,
        "/path/to/pipeline-results");

    @Before
    public void setUp() throws IOException {
        testDir = directoryRule.directory();
        archiveDir = testDir.resolve("testTar");
        Files.createDirectories(archiveDir);
        testRegularFile = testDir.resolve("regular-file.txt");
        Files.createFile(testRegularFile);
        testSubdir = testDir.resolve("sub-dir");
        Files.createDirectories(testSubdir);
        testSubdirRegularFile = testSubdir.resolve("another-regular-file.txt");
        Files.createFile(testSubdirRegularFile);
    }

    @Test
    public void testRegularFilesInDirTree() throws IOException {

        // Get a file to find size of
        Path testDirPath = testDir;
        Path testSrcFile = ZiggyUnitTestUtils.TEST_DATA.resolve("configuration")
            .resolve("pipeline-definition.xml");
        Path testFile = testDir.resolve("pipeline-definition.xml");
        Files.copy(testSrcFile, testFile);

        // Real file in a subdirectory
        Path subDirTestFile = testSubdir.resolve(Paths.get("pipeline-definition.xml"));
        Files.copy(testSrcFile, subDirTestFile);

        // Symlinked file in a real directory
        Files.createSymbolicLink(testSubdir.resolve(Paths.get("pipeline-definition-symlink.xml")),
            testSrcFile);

        // Symbolic link to a directory
        Path symlinkSubdir = testDirPath.resolve(Paths.get("symlink-sub-dir"));
        Files.createSymbolicLink(symlinkSubdir, testSubdir.toAbsolutePath());

        Map<Path, Path> regularFiles = FileUtil.regularFilesInDirTree(testDirPath);
        assertEquals(8, regularFiles.size());

        // Test that all the mapped files are as expected: top-level directory
        Path valuePath = regularFiles.get(testDirPath.relativize(testFile));
        assertNotNull(valuePath);
        assertEquals(testFile.toAbsolutePath(), valuePath);

        valuePath = regularFiles.get(testDirPath.relativize(testRegularFile));
        assertNotNull(valuePath);
        assertEquals(testRegularFile.toAbsolutePath(), valuePath);

        // sub-dir directory
        valuePath = regularFiles.get(testDirPath.relativize(testSubdirRegularFile));
        assertNotNull(valuePath);
        assertEquals(testSubdirRegularFile.toAbsolutePath(), valuePath);

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
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir)));
        FileUtil.setPosixPermissionsRecursively(testDir, "r--------", "r-x------");
        assertEquals("r-x------",
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testDir)));
        assertEquals("r-x------",
            PosixFilePermissions.toString(Files.getPosixFilePermissions(testSubdir)));
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
        FileUtil.cleanDirectoryTree(testDir);
        assertTrue(Files.isDirectory(testDir));
        assertFalse(Files.exists(archiveDir));
        assertFalse(Files.exists(testSubdir));
    }
}
