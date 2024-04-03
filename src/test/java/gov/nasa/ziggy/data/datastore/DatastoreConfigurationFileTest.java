package gov.nasa.ziggy.data.datastore;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_TEST_WORKING_DIR;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.util.io.FileUtil;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;

/**
 * Unit tests for the {@link DatastoreConfigurationFile} class.
 *
 * @author PT
 */
public class DatastoreConfigurationFileTest {

    private File schemaFile;
    private File xmlUnmarshalingFile;

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    public ZiggyPropertyRule ziggyTestWorkingDirPropertyRule = new ZiggyPropertyRule(
        ZIGGY_TEST_WORKING_DIR, directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(ziggyTestWorkingDirPropertyRule);

    @Before
    public void setUp() {
        xmlUnmarshalingFile = TEST_DATA.resolve("datastore").resolve("pd-test-1.xml").toFile();
        schemaFile = directoryRule.directory().resolve("pipeline-file.xsd").toFile();
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(DatastoreConfigurationFile.class);
        context.generateSchema(new DatastoreFileSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            FileUtil.ZIGGY_CHARSET);

        assertContains(schemaContent,
            "<xs:element name=\"datastoreConfiguration\" type=\"datastoreConfigurationFile\"/>");

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"dataFileType\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"location\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"fileNameRegexp\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"includeAllFilesInAllSubtasks\" type=\"xs:boolean\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelType\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"type\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"fileNameRegex\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"versionNumberGroup\" type=\"xs:int\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"timestampGroup\" type=\"xs:int\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"semanticVersionNumber\" type=\"xs:boolean\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"datastoreRegexp\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"value\" type=\"xs:string\" use=\"required\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"datastoreNode\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"datastoreNode\" type=\"datastoreNode\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"isRegexp\" type=\"xs:boolean\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"nodes\" type=\"xs:string\"/>");
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(DatastoreConfigurationFile.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        DatastoreConfigurationFile datastoreConfigurationFile = (DatastoreConfigurationFile) unmarshaller
            .unmarshal(xmlUnmarshalingFile);
        assertEquals(2, datastoreConfigurationFile.getDataFileTypes().size());
        assertEquals(2, datastoreConfigurationFile.getModelTypes().size());
    }

    private class DatastoreFileSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }
    }
}
