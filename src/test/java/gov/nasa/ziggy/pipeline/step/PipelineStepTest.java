package gov.nasa.ziggy.pipeline.step;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import gov.nasa.ziggy.data.management.DataFileTestUtils;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.SchemaOutputResolver;
import jakarta.xml.bind.Unmarshaller;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Unit tests for {@link PipelineStep} class. These test the XML mapping, as the rest of the class
 * is just getters, setters, and Hibernate annotations.
 *
 * @author PT
 */
public class PipelineStepTest {

    private TestPipelineStep step1, step2;
    private File xmlFile;
    private String step1XmlString, step2XmlString;
    private File xmlUnmarshalingFile1, xmlUnmarshalingFile2;
    private File schemaFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        // Set the working directory
        xmlUnmarshalingFile1 = TEST_DATA.resolve("step1.xml").toFile();
        xmlUnmarshalingFile2 = TEST_DATA.resolve("step2.xml").toFile();
        xmlFile = directoryRule.directory().resolve("steps.xml").toFile();
        schemaFile = directoryRule.directory().resolve("steps.xsd").toFile();

        // Step 1 uses defaults for everything possible
        step1 = new TestPipelineStep("step 1");
        step1.setDescription("first step");
        step1XmlString = """
            <step name="step 1" description="first step" \
            pipelineStepExecutorClass="gov.nasa.ziggy.pipeline.step.AlgorithmPipelineStepExecutor" \
            inputsClass="gov.nasa.ziggy.pipeline.step.io.DatastoreDirectoryPipelineInputs" \
            outputsClass="gov.nasa.ziggy.pipeline.step.io.DatastoreDirectoryPipelineOutputs"/>""";

        // Step 2 uses no defaults
        step2 = new TestPipelineStep("step 2");
        step2.setDescription("second step");
        step2.setInputsClass(new ClassWrapper<>(DataFileTestUtils.PipelineInputsSample.class));
        step2.setOutputsClass(new ClassWrapper<>(DataFileTestUtils.PipelineOutputsSample.class));
        step2.setPipelineStepExecutorClass(new ClassWrapper<>(AlgorithmPipelineStepExecutor.class));
        step2.setFile("executable2");
        step2XmlString = """
            <step name="step 2" description="second step" \
            pipelineStepExecutorClass="gov.nasa.ziggy.pipeline.step.AlgorithmPipelineStepExecutor" \
            inputsClass="gov.nasa.ziggy.data.management.DataFileTestUtils$PipelineInputsSample" \
            outputsClass="gov.nasa.ziggy.data.management.DataFileTestUtils$PipelineOutputsSample" \
            file="executable2"/>""";
    }

    @Test
    public void testMarshaling() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(TestPipelineStep.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(step1, xmlFile);
        assertTrue(xmlFile.exists());
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);
        assertContains(step1XmlString, xmlContent);

        marshaller.marshal(step2, xmlFile);
        assertTrue(xmlFile.exists());
        xmlContent = Files.readAllLines(xmlFile.toPath(), ZiggyFileUtils.ZIGGY_CHARSET);
        assertContains(step2XmlString, xmlContent);
    }

    @Test
    public void testUnmarshaling()
        throws JAXBException, IllegalArgumentException, IllegalAccessException {
        JAXBContext context = JAXBContext.newInstance(TestPipelineStep.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        TestPipelineStep step = (TestPipelineStep) unmarshaller.unmarshal(xmlUnmarshalingFile1);
        assertEquals(step1, step);

        step = (TestPipelineStep) unmarshaller.unmarshal(xmlUnmarshalingFile2);
        assertEquals(step2, step);
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(TestPipelineStep.class);
        context.generateSchema(new PipelineStepSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);
        assertContains("<xs:element name=\"step\" type=\"testPipelineStep\"/>", schemaContent);
        List<String> pipelineStepContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineStep\">");
        assertContains("<xs:sequence/>", pipelineStepContent);

        String[] pipelineStepAttributes = {
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            "<xs:attribute name=\"description\" type=\"xs:string\"/>",
            "<xs:attribute name=\"pipelineStepExecutorClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"inputsClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"outputsClass\" type=\"xs:string\"/>",
            "<xs:attribute name=\"file\" type=\"xs:string\"/>" };
        for (String pipelineStepAttribute : pipelineStepAttributes) {
            assertContains(pipelineStepAttribute, pipelineStepContent);
        }
    }

    private class PipelineStepSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }
    }

    /**
     * Subclass of {@link PipelineStep} that allows an XmlRootElement annotation to be prepended.
     * This allows tests of the {@link PipelineStep} class as though it was a valid root element,
     * while not forcing the non-test use-cases to put up with the class being a root element.
     *
     * @author PT
     */
    @XmlRootElement(name = "step")
    @XmlAccessorType(XmlAccessType.NONE)
    private static class TestPipelineStep extends PipelineStep {

        @SuppressWarnings("unused")
        public TestPipelineStep() {
        }

        public TestPipelineStep(String name) {
            super(name);
        }
    }
}
