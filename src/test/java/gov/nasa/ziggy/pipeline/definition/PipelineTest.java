package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.compareParameterSetReferences;
import static gov.nasa.ziggy.XmlUtils.compareXmlReferences;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.XmlUtils.nodeContent;
import static gov.nasa.ziggy.XmlUtils.pipelineContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.xml.XmlReference.InputTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ModelTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.OutputTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ParameterSetReference;
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
 * Unit tests for {@link Pipeline} class. These tests exercise the XML interface, since most of the
 * rest of the class is getters and setters.
 *
 * @author PT
 */
public class PipelineTest {

    private TestPipeline pipeline1;
    private File xmlFile;
    private File xmlUnmarshalingFile;
    private File schemaFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        xmlUnmarshalingFile = TEST_DATA.resolve("pipeline-definition.xml").toFile();
        xmlFile = directoryRule.directory().resolve("pipeline-definition.xml").toFile();
        schemaFile = directoryRule.directory().resolve("pipeline-definition.xsd").toFile();

        // Create some nodes
        PipelineNode node1 = new PipelineNode("node 1", null);
        node1.addXmlReference(new ParameterSetReference("Remote execution"));
        node1.addXmlReference(new ParameterSetReference("Convergence criteria"));
        node1.addXmlReference(new InputTypeReference("flight L0 data"));
        node1.addXmlReference(new InputTypeReference("target pixel table"));
        node1.addXmlReference(new OutputTypeReference("flight L1 data"));
        node1.addXmlReference(new ModelTypeReference("calibration constants"));

        PipelineNode node2 = new PipelineNode("node 2", null);
        node2.addXmlReference(new ParameterSetReference("Remote execution"));
        node2.addXmlReference(new ParameterSetReference("Convergence criteria"));
        node2.addXmlReference(new InputTypeReference("flight L1 data"));
        node2.addXmlReference(new OutputTypeReference("flight L2 data"));
        node2.addXmlReference(new ModelTypeReference("georeferencing constants"));

        PipelineNode node3 = new PipelineNode("node 3", null);
        node3.addXmlReference(new ParameterSetReference("Excluded bands"));
        node3.addXmlReference(new InputTypeReference("flight L1 data"));
        node3.addXmlReference(new OutputTypeReference("flight L2 data"));
        node3.addXmlReference(new ModelTypeReference("Temperature references"));

        node1.addNextNode(node2);
        node1.addNextNode(node3);

        PipelineNode node4 = new PipelineNode("node 4", null);
        node4.addXmlReference(new ParameterSetReference("Export format"));
        node4.addXmlReference(new InputTypeReference("flight L2 data"));
        node4.addXmlReference(new OutputTypeReference("exports"));

        node2.addNextNode(node4);

        pipeline1 = new TestPipeline("pipeline 1");
        pipeline1.addRootNode(node1);
        pipeline1.setDescription("first pipeline");
        pipeline1.addParameterSetName("Pipeline parameters");
        pipeline1.setInstancePriority(Priority.LOW);
    }

    @Test
    public void testMarshaller() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(TestPipeline.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        pipeline1.populateXmlFields();
        marshaller.marshal(pipeline1, xmlFile);
        assertTrue(xmlFile.exists());
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);

        List<String> pipelineContents = pipelineContent(xmlContent,
            "<pipeline name=\"pipeline 1\" description=\"first pipeline\" "
                + "instancePriority=\"LOW\" rootNodeNames=\"node 1\">");
        assertContains("<parameterSet name=\"Pipeline parameters\"/>", pipelineContents);
        List<String> nodeContents = nodeContent(pipelineContents, "<node "
            + "name=\"node 1\" childNodeNames=\"node 2, node 3\" singleSubtask=\"false\">");
        assertContains("<parameterSet name=\"Convergence criteria\"/>", nodeContents);
        assertContains("<parameterSet name=\"Remote execution\"/>", nodeContents);
        assertContains("<inputDataFileType name=\"flight L0 data\"/>", nodeContents);
        assertContains("<inputDataFileType name=\"target pixel table\"/>", nodeContents);
        assertContains("<outputDataFileType name=\"flight L1 data\"/>", nodeContents);
        nodeContents = nodeContent(pipelineContents,
            "<node " + "name=\"node 2\" childNodeNames=\"node 4\" singleSubtask=\"false\">");
        assertContains("<parameterSet name=\"Convergence criteria\"/>", nodeContents);
        assertContains("<parameterSet name=\"Remote execution\"/>", nodeContents);
        assertContains("<inputDataFileType name=\"flight L1 data\"/>", nodeContents);
        assertContains("<outputDataFileType name=\"flight L2 data\"/>", nodeContents);
        assertContains("<modelType name=\"georeferencing constants\"/>", nodeContents);
        nodeContents = nodeContent(pipelineContents,
            "<node name=\"node 3\" singleSubtask=\"false\">");
        assertContains("<parameterSet name=\"Excluded bands\"/>", nodeContents);
        assertContains("<inputDataFileType name=\"flight L1 data\"/>", nodeContents);
        assertContains("<outputDataFileType name=\"flight L2 data\"/>", nodeContents);
        assertContains("<modelType name=\"Temperature references\"/>", nodeContents);
        nodeContents = nodeContent(pipelineContents,
            "<node name=\"node 4\" singleSubtask=\"false\">");
        assertContains("<parameterSet name=\"Export format\"/>", nodeContents);
        assertContains("<inputDataFileType name=\"flight L2 data\"/>", nodeContents);
        assertContains("<outputDataFileType name=\"exports\"/>", nodeContents);
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(TestPipeline.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        Pipeline pipeline = (TestPipeline) unmarshaller.unmarshal(xmlUnmarshalingFile);
        pipeline1.populateXmlFields();
        comparePipelines(pipeline1, pipeline);
    }

    private void comparePipelines(Pipeline groundTruthPipeline, Pipeline pipeline) {
        assertEquals(groundTruthPipeline.getDescription(), pipeline.getDescription());
        assertEquals(groundTruthPipeline.getInstancePriority(), pipeline.getInstancePriority());
        assertEquals(groundTruthPipeline.getRootNodeNames(), pipeline.getRootNodeNames());
        assertEquals(groundTruthPipeline.parameterSetNamesFromXml().size(),
            pipeline.parameterSetNamesFromXml().size());
        for (String paramSetName : pipeline.parameterSetNamesFromXml()) {
            assertTrue(groundTruthPipeline.parameterSetNamesFromXml().contains(paramSetName));
        }
        for (PipelineNode node : pipeline.getNodes()) {
            checkNode(node, groundTruthPipeline.getNodes());
        }
    }

    private void checkNode(PipelineNode node, Collection<PipelineNode> groundTruthNodes) {
        PipelineNode groundTruthNode = null;
        for (PipelineNode loopNode : groundTruthNodes) {
            if (loopNode.getPipelineStepName().equals(node.getPipelineStepName())) {
                groundTruthNode = loopNode;
                break;
            }
        }
        assertFalse(groundTruthNode == null);
        assertEquals(groundTruthNode.getChildNodeNames(), node.getChildNodeNames());
        compareXmlReferences(groundTruthNode.getModelTypeReferences(),
            node.getModelTypeReferences());
        compareXmlReferences(groundTruthNode.getInputDataFileTypeReferences(),
            node.getInputDataFileTypeReferences());
        compareXmlReferences(groundTruthNode.getOutputDataFileTypeReferences(),
            node.getOutputDataFileTypeReferences());
        compareParameterSetReferences(groundTruthNode.getXmlParameterSetNames(),
            node.getXmlParameterSetNames());
    }

    @Test
    public void testGenerateSchema() throws IOException, JAXBException {
        JAXBContext context = JAXBContext.newInstance(TestPipeline.class);
        context.generateSchema(new PipelineListSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);
        assertContains("<xs:element name=\"pipeline\" type=\"testPipeline\"/>", schemaContent);

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipeline\">");
        assertContains("<xs:element name=\"node\" type=\"pipelineNode\"/>", complexTypeContent);
        assertContains("<xs:element name=\"parameterSet\" " + "type=\"parameterSetReference\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"instancePriority\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"rootNodeNames\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineNode\">");
        assertContains("<xs:element name=\"parameterSet\" " + "type=\"parameterSetReference\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"inputDataFileType\" type=\"inputTypeReference\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"outputDataFileType\" type=\"outputTypeReference\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"modelType\" type=\"modelTypeReference\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"childNodeNames\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"maxWorkers\" type=\"xs:int\"/>", complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"inputTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"outputTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"xmlReference\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
    }

    private class PipelineListSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }
    }

    /**
     * Subclass of {@link Pipeline} that allows an XmlRootElement annotation to be prepended. This
     * allows tests of the {@link Pipeline} class as though it was a valid root element, while not
     * forcing the non-test use-cases to put up with the class being a root element.
     *
     * @author PT
     */
    @XmlRootElement(name = "pipeline")
    @XmlAccessorType(XmlAccessType.NONE)
    private static class TestPipeline extends Pipeline {

        @SuppressWarnings("unused")
        public TestPipeline() {
        }

        public TestPipeline(String name) {
            super(name);
        }
    }
}
