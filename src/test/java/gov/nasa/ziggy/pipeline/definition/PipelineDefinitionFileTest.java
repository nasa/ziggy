package gov.nasa.ziggy.pipeline.definition;

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
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;

/**
 * Unit tests for {@link PipelineDefinitionFile} class.
 *
 * @author PT
 */
public class PipelineDefinitionFileTest {

    private File schemaFile;
    private File xmlUnmarshalingFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        // Set the working directory
        xmlUnmarshalingFile = TEST_DATA.resolve("configuration")
            .resolve("pd-hyperion.xml")
            .toFile();
        schemaFile = directoryRule.directory().resolve("pipeline-file.xsd").toFile();
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineDefinitionFile.class);
        context.generateSchema(new PipelineFileSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);

        assertContains(schemaContent,
            "<xs:element name=\"pipelineDefinition\" type=\"pipelineDefinitionFile\"/>");

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinitionFile\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"module\" type=\"pipelineModuleDefinition\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"pipeline\" type=\"pipelineDefinition\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineModuleDefinition\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"description\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"pipelineModuleClass\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"inputsClass\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"outputsClass\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"exeTimeoutSecs\" type=\"xs:int\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"minMemoryMegabytes\" type=\"xs:int\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinition\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"node\" type=\"pipelineDefinitionNode\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"instancePriority\" type=\"xs:string\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"rootNodeNames\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"parameterSet\" type=\"parameterSetReference\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinitionNode\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"parameterSet\" type=\"parameterSetReference\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"inputDataFileType\" type=\"inputTypeReference\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"outputDataFileType\" type=\"outputTypeReference\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"modelType\" type=\"modelTypeReference\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"moduleName\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"childNodeNames\" type=\"xs:string\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"inputTypeReference\">");
        assertContains(complexTypeContent, "<xs:extension base=\"xmlReference\">");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"outputTypeReference\">");
        assertContains(complexTypeContent, "<xs:extension base=\"xmlReference\">");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelTypeReference\">");
        assertContains(complexTypeContent, "<xs:extension base=\"xmlReference\">");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetReference\">");
        assertContains(complexTypeContent, "<xs:extension base=\"xmlReference\">");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"xmlReference\">");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(PipelineDefinitionFile.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        PipelineDefinitionFile pipelineDefinitionFile = (PipelineDefinitionFile) unmarshaller
            .unmarshal(xmlUnmarshalingFile);
        assertEquals(1, pipelineDefinitionFile.getPipelines().size());
        assertEquals(2, pipelineDefinitionFile.getModules().size());
    }

    private class PipelineFileSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }
    }
}
