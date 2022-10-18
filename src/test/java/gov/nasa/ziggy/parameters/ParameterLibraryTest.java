package gov.nasa.ziggy.parameters;

import static gov.nasa.ziggy.pipeline.definition.XmlUtils.assertContains;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.complexTypeContent;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Unit tests for the {@link ParameterLibrary} class.
 *
 * @author PT
 */
public class ParameterLibraryTest {

    private File schemaFile;
    private File xmlUnmarshalingFile;

    @Before
    public void setUp() {

        // Set the working directory
        xmlUnmarshalingFile = new File("test/data/paramlib/pl-hyperion.xml");
        new File(Filenames.BUILD_TEST).mkdirs();
        System.setProperty(PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME, Filenames.BUILD_TEST);
        schemaFile = new File(Filenames.BUILD_TEST, "param-lib.xsd");
    }

    @After
    public void tearDown() throws IOException {
        schemaFile.delete();
        System.clearProperty(PropertyNames.ZIGGY_TEST_WORKING_DIR_PROP_NAME);
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
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
