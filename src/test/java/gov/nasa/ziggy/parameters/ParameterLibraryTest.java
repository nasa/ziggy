package gov.nasa.ziggy.parameters;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
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

import gov.nasa.ziggy.ZiggyDirectoryRule;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;

/**
 * Unit tests for the {@link ParameterLibrary} class.
 *
 * @author PT
 */
public class ParameterLibraryTest {

    private File schemaFile;
    private File xmlUnmarshalingFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        // Set the working directory
        xmlUnmarshalingFile = TEST_DATA.resolve("paramlib").resolve("pl-hyperion.xml").toFile();
        schemaFile = directoryRule.directory().resolve("param-lib.xsd").toFile();
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(ParameterLibrary.class);
        context.generateSchema(new ParamLibSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath());

        assertContains(schemaContent,
            "<xs:element name=\"parameterLibrary\" type=\"parameterLibrary\"/>");

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterLibrary\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"parameter-set\" type=\"parameterSet\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"release\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"repository-revision\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"database-user\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"build-date\" type=\"xs:anySimpleType\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"override-only\" type=\"xs:boolean\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"database-url\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"repository-branch\" type=\"xs:string\"/> ");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSet\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"parameter\" type=\"parameter\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"version\" type=\"xs:int\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"locked\" type=\"xs:boolean\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"classname\" type=\"xs:string\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameter\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"value\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent, "<xs:attribute name=\"type\" type=\"xs:string\"/>");

    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(ParameterLibrary.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        ParameterLibrary paramLib = (ParameterLibrary) unmarshaller.unmarshal(xmlUnmarshalingFile);
        assertEquals(5, paramLib.getParameterSets().size());
    }

    private class ParamLibSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }

    }

}
