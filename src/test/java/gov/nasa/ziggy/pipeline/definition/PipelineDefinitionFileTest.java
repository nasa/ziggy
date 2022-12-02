package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.pipeline.definition.XmlUtils.assertContains;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.complexTypeContent;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.util.io.Filenames;

/**
 * Unit tests for {@link PipelineDefinitionFile} class.
 *
 * @author PT
 */
public class PipelineDefinitionFileTest {

    private String workingDirName;
    private File schemaFile;
    private File xmlUnmarshalingFile;

    @Before
    public void setUp() {

        // Set the working directory
        workingDirName = System.getProperty("user.dir");
        xmlUnmarshalingFile = new File(workingDirName + "/test/data/configuration/pd-hyperion.xml");
        String workingDir = workingDirName + "/build/test";
        new File(workingDir).mkdirs();
        schemaFile = new File(workingDir, "pipeline-file.xsd");
    }

    @After
    public void tearDown() throws IOException {
        schemaFile.delete();
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineDefinitionFile.class);
        context.generateSchema(new PipelineFileSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath());

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
            "<xs:attribute name=\"minMemoryMegaBytes\" type=\"xs:int\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinition\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"node\" type=\"pipelineDefinitionNode\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"instancePriority\" type=\"xs:int\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"rootNodeNames\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"pipelineParameter\" type=\"parameterSetName\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetName\">");
        assertContains(complexTypeContent, "<xs:extension base=\"xmlReference\">");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinitionNode\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"moduleParameter\" type=\"parameterSetName\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"inputDataFileType\" type=\"inputTypeReference\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"outputDataFileType\" type=\"outputTypeReference\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"modelType\" type=\"modelTypeReference\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"startNewUow\" type=\"xs:boolean\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"uowGenerator\" type=\"xs:string\"/>");
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
        assertEquals(4, pipelineDefinitionFile.getModules().size());
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
