package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
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
        ZIGGY_TEST_WORKING_DIR_PROP_NAME, directoryRule);

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
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath());

        assertContains(schemaContent,
            "<xs:element name=\"datastoreConfiguration\" type=\"datastoreConfigurationFile\"/>");

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"dataFileType\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"fileNameRegexForTaskDir\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"fileNameWithSubstitutionsForDatastore\" type=\"xs:string\" use=\"required\"/>");

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
            "<xs:complexType name=\"dataFileType\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"fileNameRegexForTaskDir\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"fileNameWithSubstitutionsForDatastore\" type=\"xs:string\" use=\"required\"/>");
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(DatastoreConfigurationFile.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        DatastoreConfigurationFile datastoreConfigurationFile = (DatastoreConfigurationFile) unmarshaller
            .unmarshal(xmlUnmarshalingFile);
        assertEquals(5, datastoreConfigurationFile.getDataFileTypes().size());
        assertEquals(2, datastoreConfigurationFile.getModelTypes().size());

        Set<DataFileType> dataFileTypes = datastoreConfigurationFile.getDataFileTypes();
        for (DataFileType dataFileType : dataFileTypes) {
            if (dataFileType.getName().equals("has backslashes")) {
                assertEquals("(\\S+)-(set-[0-9])-(file-[0-9]).png",
                    dataFileType.getFileNameRegexForTaskDir());
            }
        }
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
