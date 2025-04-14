package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.XmlUtils.simpleTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.DATASTORE_ROOT_DIR;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.Manifest.ManifestEntry;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import jakarta.xml.bind.JAXBException;

/**
 * Unit tests for {@link Manifest} class.
 *
 * @author PT
 */
public class ManifestTest {

    public static final String TEST_DATA_DIR = "manifest";
    public static final String TEST_DATA_SRC = TEST_DATA.toString();

    private Path testDataDir;
    private Path subDir;

    @Rule
    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, "/dev/null");

    @Before
    public void setUp() {
        testDataDir = ziggyDirectoryRule.directory().resolve(TEST_DATA_DIR);
        testDataDir.toFile().mkdirs();
        subDir = testDataDir.resolve("sub-directory");
    }

    @Test
    public void testGenerateFromFiles() throws IOException {

        // Copy all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, testDataDir.resolve(xmlFile.getFileName()));
        }

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        Files.createDirectory(subDir);
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, subDir.resolve(xmlFile.getFileName()));
        }

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        assertEquals(100L, manifest.getDatasetId());
        assertEquals(54, manifest.getFileCount());
        assertEquals(54, manifest.getManifestEntries().size());
        for (ManifestEntry manifestFile : manifest.getManifestEntries()) {
            validateManifestFile(manifestFile);
        }
    }

    @Test
    public void testGenerateFromSymlinks() throws IOException {

        // Symlink all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        for (Path xmlFile : xmlFiles) {
            Path destPath = testDataDir.resolve(xmlFile.getFileName());
            Files.createSymbolicLink(destPath, xmlFile);
        }

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        Files.createDirectory(subDir);
        for (Path xmlFile : xmlFiles) {
            Path destPath = subDir.resolve(xmlFile.getFileName());
            Files.createSymbolicLink(destPath, xmlFile);
        }
        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        assertEquals(100L, manifest.getDatasetId());
        assertEquals(54, manifest.getFileCount());
        assertEquals(54, manifest.getManifestEntries().size());
        for (ManifestEntry manifestFile : manifest.getManifestEntries()) {
            validateManifestFile(manifestFile);
        }
    }

    @Test
    public void testWriteAndReadManifest() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Copy all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, testDataDir.resolve(xmlFile.getFileName()));
        }

        // Create a couple of files that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        Files.createDirectory(subDir);
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, subDir.resolve(xmlFile.getFileName()));
        }

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        manifest.write(testDataDir);
        assertTrue(Files.exists(testDataDir.resolve("test-manifest.xml")));

        // Read the manifest back in
        Manifest roundTripManifest = Manifest.readManifest(testDataDir);
        assertEquals("test-manifest.xml", roundTripManifest.getName());
        assertEquals(54, roundTripManifest.getFileCount());
        assertEquals(54, roundTripManifest.getManifestEntries().size());
        assertEquals(100L, roundTripManifest.getDatasetId());
        for (ManifestEntry manifestFile : roundTripManifest.getManifestEntries()) {
            assertTrue(manifest.getManifestEntries().contains(manifestFile));
        }
    }

    @Test
    public void testWriteAndReadManifestSymlinks() throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException, IllegalArgumentException,
        InvocationTargetException, NoSuchMethodException, SecurityException {

        // Copy all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, testDataDir.resolve(xmlFile.getFileName()));
        }

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        Files.createDirectory(subDir);
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, subDir.resolve(xmlFile.getFileName()));
        }

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        manifest.write(testDataDir);
        assertTrue(Files.exists(testDataDir.resolve("test-manifest.xml")));

        // Read the manifest back in
        Manifest roundTripManifest = Manifest.readManifest(testDataDir);
        assertEquals("test-manifest.xml", roundTripManifest.getName());
        assertEquals(54, roundTripManifest.getFileCount());
        assertEquals(54, roundTripManifest.getManifestEntries().size());
        assertEquals(100L, roundTripManifest.getDatasetId());
        for (ManifestEntry manifestFile : roundTripManifest.getManifestEntries()) {
            assertTrue(manifest.getManifestEntries().contains(manifestFile));
        }
    }

    @Test
    public void testSchema() throws IOException {
        Path schemaPath = Paths.get(
            ZiggyConfiguration.getInstance().getString(ZIGGY_HOME_DIR.property()), "schema", "xml",
            new Manifest().getXmlSchemaFilename());
        List<String> schemaContent = Files.readAllLines(schemaPath, ZiggyFileUtils.ZIGGY_CHARSET);

        assertContains("<xs:element name=\"manifest\" type=\"manifest\"/>", schemaContent);

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"manifest\">");
        assertContains("<xs:element name=\"file\" type=\"manifestEntry\" maxOccurs=\"unbounded\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"datasetId\" type=\"xs:long\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"fileCount\" type=\"xs:int\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"checksumType\" type=\"checksumType\" use=\"required\"/>",
            complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"manifestEntry\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"size\" type=\"xs:long\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"checksum\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);

        complexTypeContent = simpleTypeContent(schemaContent,
            "<xs:simpleType name=\"checksumType\">");
        assertContains("<xs:restriction base=\"xs:string\">", complexTypeContent);
        assertContains("<xs:enumeration value=\"MD5\"/>", complexTypeContent);
        assertContains("<xs:enumeration value=\"SHA1\"/>", complexTypeContent);
        assertContains("<xs:enumeration value=\"SHA256\"/>", complexTypeContent);
        assertContains("<xs:enumeration value=\"SHA512\"/>", complexTypeContent);
    }

    // TODO : fix this!
    private void validateManifestFile(ManifestEntry manifestFile) throws IOException {
//        Path file = DataFileManager.realSourceFile(testDataDir.resolve(manifestFile.getName()));
//        assertTrue(Files.exists(file));
//        assertEquals(Files.size(file), manifestFile.getSize());
//        String sha256 = checksumType.checksum(file);
//        assertEquals(sha256, manifestFile.getChecksum());
    }
}
