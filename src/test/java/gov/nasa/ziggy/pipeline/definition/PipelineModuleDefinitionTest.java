package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.data.management.DataFileTestUtils;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.util.io.FileUtil;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Unit tests for {@link PipelineModuleDefinition} class. These test the XML mapping, as the rest of
 * the class is just getters, setters, and Hibernate annotations.
 *
 * @author PT
 */
public class PipelineModuleDefinitionTest {

    private PipelineMod module1, module2;
    private File xmlFile;
    private String module1XmlString, module2XmlString;
    private File xmlUnmarshalingFile1, xmlUnmarshalingFile2;
    private File schemaFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        // Set the working directory
        Path xmlUnmarshalingPath = TEST_DATA.resolve("configuration");
        xmlUnmarshalingFile1 = xmlUnmarshalingPath.resolve("module1.xml").toFile();
        xmlUnmarshalingFile2 = xmlUnmarshalingPath.resolve("module2.xml").toFile();
        xmlFile = directoryRule.directory().resolve("modules.xml").toFile();
        schemaFile = directoryRule.directory().resolve("modules.xsd").toFile();

        // Module 1 uses defaults for everything possible
        module1 = new PipelineMod("module 1");
        module1.setDescription("first module");
        module1.setExeTimeoutSecs(100);
        module1XmlString = """
            <module name="module 1" description="first module" \
            pipelineModuleClass="gov.nasa.ziggy.module.ExternalProcessPipelineModule" \
            inputsClass="gov.nasa.ziggy.module.DatastoreDirectoryPipelineInputs" \
            outputsClass="gov.nasa.ziggy.module.DatastoreDirectoryPipelineOutputs" \
            exeTimeoutSecs="100" minMemoryMegabytes="0"/>""";

        // Module 2 uses no defaults
        module2 = new PipelineMod("module 2");
        module2.setDescription("second module");
        module2.setExeTimeoutSecs(300);
        module2.setInputsClass(new ClassWrapper<>(DataFileTestUtils.PipelineInputsSample.class));
        module2.setOutputsClass(new ClassWrapper<>(DataFileTestUtils.PipelineOutputsSample1.class));
        module2.setPipelineModuleClass(new ClassWrapper<>(ExternalProcessPipelineModule.class));
        module2XmlString = """
            <module name="module 2" description="second module" \
            pipelineModuleClass="gov.nasa.ziggy.module.ExternalProcessPipelineModule" \
            inputsClass="gov.nasa.ziggy.data.management.DataFileTestUtils$PipelineInputsSample" \
            outputsClass="gov.nasa.ziggy.data.management.DataFileTestUtils$PipelineOutputsSample1" \
            exeTimeoutSecs="300" minMemoryMegabytes="0"/>""";
    }

    @Test
    public void testMarshaling() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineMod.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(module1, xmlFile);
        assertTrue(xmlFile.exists());
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath(), FileUtil.ZIGGY_CHARSET);
        assertContains(xmlContent, module1XmlString);

        marshaller.marshal(module2, xmlFile);
        assertTrue(xmlFile.exists());
        xmlContent = Files.readAllLines(xmlFile.toPath(), FileUtil.ZIGGY_CHARSET);
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
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            FileUtil.ZIGGY_CHARSET);
        assertContains(schemaContent, "<xs:element name=\"module\" type=\"pipelineMod\"/>");
        List<String> moduleDefContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineModuleDefinition\">");
        assertContains(moduleDefContent, "<xs:sequence/>");

        String[] moduleDefAttributes = {
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            "<xs:attribute name=\"description\" type=\"xs:string\"/>",
            "<xs:attribute name=\"pipelineModuleClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"inputsClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"outputsClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"exeTimeoutSecs\" type=\"xs:int\"/>",
            "<xs:attribute name=\"minMemoryMegabytes\" type=\"xs:int\"/>" };
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

        @SuppressWarnings("unused")
        public PipelineMod() {
        }

        public PipelineMod(String name) {
            super(name);
        }
    }
}
