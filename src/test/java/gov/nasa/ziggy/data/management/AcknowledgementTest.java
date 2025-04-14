package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.XmlUtils.simpleTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.data.management.Manifest.manifestEntryAckEntryEquals;
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
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.data.management.Acknowledgement.AcknowledgementEntry;
import gov.nasa.ziggy.data.management.Manifest.ManifestEntry;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Unit tests for the {@link Acknowledgement} class.
 *
 * @author PT
 */
public class AcknowledgementTest {

    public static final String TEST_DATA_DIR = "manifest";

    private Path testDataDir;

    @Rule
    public ZiggyDirectoryRule dirRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Rule
    public ZiggyPropertyRule datastoreRootDirPropertyRule = new ZiggyPropertyRule(
        DATASTORE_ROOT_DIR, "/dev/null");

    @Before
    public void setUp() throws IOException {
        testDataDir = dirRule.directory().resolve(TEST_DATA_DIR);
        Files.createDirectories(testDataDir);
        AlertService.setInstance(Mockito.mock(AlertService.class));
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        AlertService.setInstance(null);
    }

    @Test
    public void testGenerateAcknowledgement() throws IOException {

        // Copy all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, testDataDir.resolve(xmlFile.getFileName()));
        }

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        Acknowledgement ack = Acknowledgement.of(manifest, testDataDir,
            new PipelineTask(null, null, null));
        assertEquals("test-manifest-ack.xml", ack.getName());
        assertEquals(27, ack.getFileCount());
        assertEquals(100L, ack.getDatasetId());
        validateAckFiles(manifest.fileNameToManifestEntry(), ack.fileNameToAckEntry(), null);
        assertEquals(DataReceiptStatus.VALID, ack.getTransferStatus());
        assertEquals(DataReceiptStatus.VALID, manifest.getStatus());
    }

    @Test
    public void testGenerateAcknowledgementFromSymlinks() throws IOException {

        // Symlink all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        symlinkFilesToTestDataDir(xmlFiles);

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        createSubdirectoryWithContent("sub-directory", xmlFiles);

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        Acknowledgement ack = Acknowledgement.of(manifest, testDataDir,
            new PipelineTask(null, null, null));
        assertEquals("test-manifest-ack.xml", ack.getName());
        assertEquals(54, ack.getFileCount());
        assertEquals(100L, ack.getDatasetId());
        validateAckFiles(manifest.fileNameToManifestEntry(), ack.fileNameToAckEntry(), null);
        assertEquals(DataReceiptStatus.VALID, ack.getTransferStatus());
        assertEquals(DataReceiptStatus.VALID, manifest.getStatus());
    }

    private void symlinkFilesToTestDataDir(Set<Path> xmlFiles) throws IOException {
        for (Path xmlFile : xmlFiles) {
            Path destPath = testDataDir.resolve(xmlFile.getFileName());
            Files.createSymbolicLink(destPath, xmlFile);
        }
    }

    private void createSubdirectoryWithContent(String subdirectoryName, Set<Path> xmlFiles)
        throws IOException {
        Files.createDirectory(testDataDir.resolve(subdirectoryName));
        for (Path xmlFile : xmlFiles) {
            Path destPath = testDataDir.resolve(subdirectoryName).resolve(xmlFile.getFileName());
            Files.createSymbolicLink(destPath, xmlFile);
        }
    }

    @Test
    public void testFailedTransferStatus() throws IOException {

        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        symlinkFilesToTestDataDir(xmlFiles);

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        createSubdirectoryWithContent("sub-directory", xmlFiles);

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        manifest.getManifestEntries().get(0).setName("dummy-name.rpi");
        Acknowledgement ack = Acknowledgement.of(manifest, testDataDir,
            new PipelineTask(null, null, null));
        assertEquals("test-manifest-ack.xml", ack.getName());
        assertEquals(54, ack.getFileCount());
        assertEquals(100L, ack.getDatasetId());
        Map<String, ManifestEntry> mFileMap = manifest.fileNameToManifestEntry();
        Map<String, AcknowledgementEntry> aFileMap = ack.fileNameToAckEntry();
        validateAckFiles(mFileMap, aFileMap, "dummy-name.rpi");
        AcknowledgementEntry aFile = aFileMap.get("dummy-name.rpi");
        assertEquals(DataReceiptStatus.ABSENT, aFile.getTransferStatus());
        assertEquals(DataReceiptStatus.INVALID, aFile.getValidationStatus());
        assertEquals(0, aFile.getSize());
        assertEquals("", aFile.getChecksum());
        assertEquals(DataReceiptStatus.INVALID, ack.getTransferStatus());
        assertEquals(DataReceiptStatus.INVALID, manifest.getStatus());
    }

    @Test
    public void testFailedSizeValidation() throws IOException {

        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        symlinkFilesToTestDataDir(xmlFiles);

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        createSubdirectoryWithContent("sub-directory", xmlFiles);

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        String testFileName = manifest.getManifestEntries().get(0).getName();
        long trueSize = manifest.getManifestEntries().get(0).getSize();
        manifest.getManifestEntries().get(0).setSize(trueSize + 1);
        Acknowledgement ack = Acknowledgement.of(manifest, testDataDir,
            new PipelineTask(null, null, null));
        assertEquals("test-manifest-ack.xml", ack.getName());
        assertEquals(54, ack.getFileCount());
        assertEquals(100L, ack.getDatasetId());
        Map<String, ManifestEntry> mFileMap = manifest.fileNameToManifestEntry();
        Map<String, AcknowledgementEntry> aFileMap = ack.fileNameToAckEntry();
        validateAckFiles(mFileMap, aFileMap, testFileName);
        AcknowledgementEntry aFile = aFileMap.get(testFileName);
        assertEquals(DataReceiptStatus.PRESENT, aFile.getTransferStatus());
        assertEquals(DataReceiptStatus.INVALID, aFile.getValidationStatus());
        assertEquals(trueSize, aFile.getSize());
        assertEquals(mFileMap.get(testFileName).getChecksum(), aFile.getChecksum());
        assertEquals(DataReceiptStatus.INVALID, ack.getTransferStatus());
        assertEquals(DataReceiptStatus.INVALID, manifest.getStatus());
    }

    @Test
    public void testFailedChecksumValidation() throws IOException {

        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        symlinkFilesToTestDataDir(xmlFiles);

        // Create a file that should not be in the manifest
        Files.createFile(testDataDir.resolve(".hidden-file"));

        // Generate a directory with content
        createSubdirectoryWithContent("sub-directory", xmlFiles);

        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");
        String testFileName = manifest.getManifestEntries().get(0).getName();
        String trueChecksum = manifest.getManifestEntries().get(0).getChecksum();
        manifest.getManifestEntries().get(0).setChecksum(trueChecksum + "0");
        Acknowledgement ack = Acknowledgement.of(manifest, testDataDir,
            new PipelineTask(null, null, null));
        assertEquals("test-manifest-ack.xml", ack.getName());
        assertEquals(54, ack.getFileCount());
        assertEquals(100L, ack.getDatasetId());
        Map<String, ManifestEntry> mFileMap = manifest.fileNameToManifestEntry();
        Map<String, AcknowledgementEntry> aFileMap = ack.fileNameToAckEntry();
        validateAckFiles(mFileMap, aFileMap, testFileName);
        AcknowledgementEntry aFile = aFileMap.get(testFileName);
        assertEquals(DataReceiptStatus.PRESENT, aFile.getTransferStatus());
        assertEquals(DataReceiptStatus.INVALID, aFile.getValidationStatus());
        assertEquals(trueChecksum, aFile.getChecksum());
        assertEquals(mFileMap.get(testFileName).getSize(), aFile.getSize());
        assertEquals(DataReceiptStatus.INVALID, ack.getTransferStatus());
        assertEquals(DataReceiptStatus.INVALID, manifest.getStatus());
    }

    @Test
    public void testXmlRoundTrip()
        throws IOException, InstantiationException, IllegalAccessException, SAXException,
        jakarta.xml.bind.JAXBException, IllegalArgumentException, InvocationTargetException,
        NoSuchMethodException, SecurityException {

        // Copy all the files from the source directory
        Set<Path> xmlFiles = ZiggyFileUtils.listFiles(TEST_DATA, "\\S+\\.xml");
        for (Path xmlFile : xmlFiles) {
            Files.copy(xmlFile, testDataDir.resolve(xmlFile.getFileName()));
        }

        // Create a manifest based on the directory contents
        Manifest manifest = Manifest.generateManifest(testDataDir, 100L);
        manifest.setName("test-manifest.xml");

        Acknowledgement ack = Acknowledgement.of(manifest, testDataDir,
            new PipelineTask(null, null, null));
        ack.write(testDataDir);

        // There's no method for reading in an acknowledgement because
        // AFAICT there's no use-case for it, so instead we can do it manually.
        ValidatingXmlManager<Acknowledgement> xmlManager = new ValidatingXmlManager<>(
            Acknowledgement.class);
        Path ackPath = testDataDir.resolve("test-manifest-ack.xml");
        Acknowledgement ack2 = xmlManager.unmarshal(ackPath.toFile());
        assertEquals(ack.getFileCount(), ack2.getFileCount());
        assertEquals(ack.getDatasetId(), ack2.getDatasetId());
        assertEquals(ack.getTransferStatus(), ack2.getTransferStatus());
        assertEquals(ack.getAcknowledgementEntries().size(),
            ack2.getAcknowledgementEntries().size());
        Map<String, AcknowledgementEntry> aFileMap = ack.fileNameToAckEntry();
        Map<String, AcknowledgementEntry> aFileMap2 = ack2.fileNameToAckEntry();
        testAcknowledgementFiles(aFileMap, aFileMap2);
    }

    @Test
    public void testSchema() throws IOException {

        Path schemaPath = Paths.get(ziggyHomeDirPropertyRule.getValue(), "schema", "xml",
            new Acknowledgement().getXmlSchemaFilename());
        List<String> schemaContent = Files.readAllLines(schemaPath, ZiggyFileUtils.ZIGGY_CHARSET);

        assertContains("<xs:element name=\"acknowledgement\" type=\"acknowledgement\"/>",
            schemaContent);

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"acknowledgement\">");
        assertContains(
            "<xs:element name=\"file\" type=\"acknowledgementEntry\" maxOccurs=\"unbounded\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"datasetId\" type=\"xs:long\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"fileCount\" type=\"xs:int\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"transferStatus\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"checksumType\" type=\"checksumType\" use=\"required\"/>",
            complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"acknowledgementEntry\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"size\" type=\"xs:long\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"checksum\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"transferStatus\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"validationStatus\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);

        complexTypeContent = simpleTypeContent(schemaContent,
            "<xs:simpleType name=\"checksumType\">");
        assertContains("<xs:restriction base=\"xs:string\">", complexTypeContent);
        assertContains("<xs:enumeration value=\"MD5\"/>", complexTypeContent);
        assertContains("<xs:enumeration value=\"SHA1\"/>", complexTypeContent);
        assertContains("<xs:enumeration value=\"SHA256\"/>", complexTypeContent);
        assertContains("<xs:enumeration value=\"SHA512\"/>", complexTypeContent);
    }

    private void testAcknowledgementFiles(Map<String, AcknowledgementEntry> aFileMap,
        Map<String, AcknowledgementEntry> aFileMap2) {

        for (String key : aFileMap.keySet()) {
            AcknowledgementEntry aFile = aFileMap.get(key);
            AcknowledgementEntry aFile2 = aFileMap2.get(key);
            assertEquals(aFile, aFile2);
        }
    }

    private void validateAckFiles(Map<String, ManifestEntry> mFileMap,
        Map<String, AcknowledgementEntry> aFileMap, String exception) {
        assertEquals(mFileMap.size(), aFileMap.size());
        for (String filename : mFileMap.keySet()) {
            if (exception != null && filename.equals(exception)) {
                continue;
            }
            ManifestEntry mFile = mFileMap.get(filename);
            AcknowledgementEntry aFile = aFileMap.get(filename);
            assertTrue(manifestEntryAckEntryEquals(mFile, aFile));
        }
    }
}
