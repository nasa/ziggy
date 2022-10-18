package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.pipeline.definition.XmlUtils.assertContains;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.complexTypeContent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.data.management.DataFileTestUtils;
import gov.nasa.ziggy.module.ExternalProcessPipelineModuleTest;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Unit tests for {@link PipelineModuleDefinition} class. These test the XML mapping, as the rest of
 * the class is just getters, setters, and Hibernate annotations.
 *
 * @author PT
 */
public class PipelineModuleDefinitionTest {

    private String workingDirName;
    private PipelineMod module1, module2;
    private File xmlFile;
    private String module1XmlString, module2XmlString;
    private File xmlUnmarshalingFile1, xmlUnmarshalingFile2;
    private File schemaFile;

    @Before
    public void setUp() {

        // Set the working directory
        workingDirName = System.getProperty("user.dir");
        xmlUnmarshalingFile1 = new File(workingDirName + "/test/data/configuration/module1.xml");
        xmlUnmarshalingFile2 = new File(workingDirName + "/test/data/configuration/module2.xml");
        String workingDir = workingDirName + "/build/test";
        new File(workingDir).mkdirs();
        xmlFile = new File(workingDir, "modules.xml");
        schemaFile = new File(workingDir, "modules.xsd");

        // Module 1 uses defaults for everything possible
        module1 = new PipelineMod("module 1");
        module1.setDescription("first module");
        module1.setExeTimeoutSecs(100);
        module1.setMinMemoryMegaBytes(200);
        module1XmlString = "<module name=\"module 1\" description=\"first module\" "
            + "pipelineModuleClass=\"gov.nasa.ziggy.module.ExternalProcessPipelineModule\" "
            + "inputsClass=\"gov.nasa.ziggy.module.DefaultPipelineInputs\" "
            + "outputsClass=\"gov.nasa.ziggy.module.DefaultPipelineOutputs\" "
            + "exeTimeoutSecs=\"100\" minMemoryMegaBytes=\"200\"/>";

        // Module 2 uses no defaults
        module2 = new PipelineMod("module 2");
        module2.setDescription("second module");
        module2.setExeTimeoutSecs(300);
        module2.setMinMemoryMegaBytes(400);
        module2.setInputsClass(new ClassWrapper<>(DataFileTestUtils.PipelineInputsSample.class));
        module2.setOutputsClass(new ClassWrapper<>(DataFileTestUtils.PipelineOutputsSample1.class));
        module2.setPipelineModuleClass(
            new ClassWrapper<>(ExternalProcessPipelineModuleTest.TestPipelineModule.class));
        module2XmlString = "<module name=\"module 2\" description=\"second module\" "
            + "pipelineModuleClass=\"gov.nasa.ziggy.module.ExternalProcessPipelineModuleTest$TestPipelineModule\" "
            + "inputsClass=\"gov.nasa.ziggy.data.management.DataFileTestUtils$PipelineInputsSample\" "
            + "outputsClass=\"gov.nasa.ziggy.data.management.DataFileTestUtils$PipelineOutputsSample1\" "
            + "exeTimeoutSecs=\"300\" minMemoryMegaBytes=\"400\"/>";
    }

    @After
    public void tearDown() throws IOException {
        xmlFile.delete();
        schemaFile.delete();
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test
    public void testMarshaling() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineMod.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(module1, xmlFile);
        assertTrue(xmlFile.exists());
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath());
        assertContains(xmlContent, module1XmlString);

        marshaller.marshal(module2, xmlFile);
        assertTrue(xmlFile.exists());
        xmlContent = Files.readAllLines(xmlFile.toPath());
        assertContains(xmlContent, module2XmlString);

    }

    @Test
    public void testUnmarshaling()
        throws JAXBException, IllegalArgumentException, IllegalAccessException {
        JAXBContext context = JAXBContext.newInstance(PipelineMod.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        PipelineMod module = (PipelineMod) unmarshaller.unmarshal(xmlUnmarshalingFile1);
        assertEquals(module1, module);

        module = (PipelineMod) unmarshaller.unmarshal(xmlUnmarshalingFile2);
        assertEquals(module2, module);
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineMod.class);
        context.generateSchema(new ModulesSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath());
        assertContains(schemaContent, "<xs:element name=\"module\" type=\"pipelineMod\"/>");
        List<String> moduleDefContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineModuleDefinition\">");
        assertContains(moduleDefContent, "<xs:sequence/>");

        String[] moduleDefAttributes = new String[] {
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            "<xs:attribute name=\"description\" type=\"xs:string\"/>",
            "<xs:attribute name=\"pipelineModuleClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"inputsClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"outputsClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"exeTimeoutSecs\" type=\"xs:int\"/>",
            "<xs:attribute name=\"minMemoryMegaBytes\" type=\"xs:int\"/>" };
        for (String moduleAttribute : moduleDefAttributes) {
            assertContains(moduleDefContent, moduleAttribute);
        }
    }

    private class ModulesSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }

    }

    /**
     * Subclass of {@link PipelineModuleDefinition} that allows an XmlRootElement annotation to be
     * prepended. This allows tests of the {@link PipelineModuleDefinition} class as though it was a
     * valid root element, while not forcing the non-test use-cases to put up with the class being a
     * root element.
     *
     * @author PT
     */
    @XmlRootElement(name = "module")
    @XmlAccessorType(XmlAccessType.NONE)
    private static class PipelineMod extends PipelineModuleDefinition {

        public PipelineMod() {

        }

        public PipelineMod(String name) {
            super(name);
        }
    }

}
