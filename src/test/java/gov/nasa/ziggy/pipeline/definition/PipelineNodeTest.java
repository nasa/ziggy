package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.XmlUtils.nodeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.pipeline.xml.XmlReference;
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
 * Unit tests for {@link PipelineNode} class. These are primarily tests of the XML conversion
 * system, since the rest of the class is just getters and setters.
 *
 * @author PT
 */
public class PipelineNodeTest {

    private Node node;
    private File xmlFile;
    private File xmlUnmarshalingFile;
    private File schemaFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        // Set the working directory
        xmlUnmarshalingFile = TEST_DATA.resolve("node.xml").toFile();
        xmlFile = directoryRule.directory().resolve("node.xml").toFile();
        schemaFile = directoryRule.directory().resolve("node.xsd").toFile();

        // Construct a new node for the test
        node = new Node("node 1", null);
        node.setChildNodeNames("node 2, node 3");
        node.setHeapSizeGigabytes(2);
        node.populateXmlFields();
        Set<XmlReference> xmlReferences = new HashSet<>();
        xmlReferences.add(new ParameterSetReference("Remote execution"));
        xmlReferences.add(new ParameterSetReference("Convergence criteria"));
        xmlReferences.add(new InputTypeReference("flight L0 data"));
        xmlReferences.add(new InputTypeReference("target pixel data"));
        xmlReferences.add(new OutputTypeReference("flight L1 data"));
        xmlReferences.add(new ModelTypeReference("calibration constants"));
        node.setXmlReferences(xmlReferences);
    }

    @Test
    public void testMarshaller() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(Node.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(node, xmlFile);
        assertTrue(xmlFile.exists());
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);
        assertEquals(9, xmlContent.size());
        List<String> nodeContent = nodeContent(xmlContent,
            "<node heapSizeGigabytes=\"2\" name=\"node 1\" "
                + "childNodeNames=\"node 2, node 3\" singleSubtask=\"false\">");
        String[] xmlLines = { "<parameterSet name=\"Convergence criteria\"/>",
            "<parameterSet name=\"Remote execution\"/>",
            "<inputDataFileType name=\"flight L0 data\"/>",
            "<outputDataFileType name=\"flight L1 data\"/>",
            "<inputDataFileType name=\"target pixel data\"/>",
            "<modelType name=\"calibration constants\"/>" };
        for (String xmlLine : xmlLines) {
            assertContains(xmlLine, nodeContent);
        }
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Node.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        PipelineNode node = (Node) unmarshaller.unmarshal(xmlUnmarshalingFile);
        assertEquals("node 1", node.getPipelineStepName());
        assertEquals("node 2, node 3", node.getChildNodeNames());
        assertEquals(2, node.getXmlParameterSetNames().size());
        assertTrue(node.getXmlParameterSetNames().contains("Remote execution"));
        assertTrue(node.getXmlParameterSetNames().contains("Convergence criteria"));
        assertEquals(2, node.getInputDataFileTypeReferences().size());
        assertTrue(node.getInputDataFileTypeReferences()
            .contains(new InputTypeReference("flight L0 data")));
        assertTrue(node.getInputDataFileTypeReferences()
            .contains(new InputTypeReference("target pixel table")));
        assertEquals(1, node.getOutputDataFileTypeReferences().size());
        assertTrue(node.getOutputDataFileTypeReferences()
            .contains(new OutputTypeReference("flight L1 data")));
        assertEquals(1, node.getModelTypeReferences().size());
        assertTrue(node.getModelTypeReferences()
            .contains(new ModelTypeReference("calibration constants")));
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(Node.class);
        context.generateSchema(new NodeSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);
        assertContains("<xs:element name=\"node\" type=\"node\"/>", schemaContent);

        List<String> nodeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"node\">");
        assertContains("<xs:extension base=\"pipelineNode\">", nodeContent);

        nodeContent = complexTypeContent(schemaContent, "<xs:complexType name=\"pipelineNode\">");
        String[] nodeStrings = {
            "<xs:element name=\"parameterSet\" type=\"parameterSetReference\"/>",
            "<xs:element name=\"inputDataFileType\" type=\"inputTypeReference\"/>",
            "<xs:element name=\"outputDataFileType\" type=\"outputTypeReference\"/>",
            "<xs:element name=\"modelType\" type=\"modelTypeReference\"/>",
            "<xs:attribute name=\"maxWorkers\" type=\"xs:int\"/>",
            "<xs:attribute name=\"heapSizeGigabytes\" type=\"xs:int\"/>",
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            "<xs:attribute name=\"singleSubtask\" type=\"xs:boolean\"/>",
            "<xs:attribute name=\"childNodeNames\" type=\"xs:string\"/>" };
        for (String nodeString : nodeStrings) {
            assertContains(nodeString, nodeContent);
        }

        List<String> xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"xmlReference\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            xmlReferenceContent);

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"inputTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", xmlReferenceContent);

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"outputTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", xmlReferenceContent);

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", xmlReferenceContent);

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", xmlReferenceContent);
    }

    @Test
    public void testNodeListConstruction() {

        PipelineNode node2 = new PipelineNode("node 2", null);
        node2.addXmlReference(new ParameterSetReference("Remote execution"));
        node2.addXmlReference(new ParameterSetReference("Convergence criteria"));
        node2.addXmlReference(new InputTypeReference("flight L1 data"));
        node2.addXmlReference(new OutputTypeReference("flight L2 data"));
        node2.addXmlReference(new ModelTypeReference("georeferencing constants"));

        node.addNextNode(node2);

        PipelineNode node3 = new PipelineNode("node 3", null);
        node3.addXmlReference(new ParameterSetReference("Excluded bands"));
        node3.addXmlReference(new InputTypeReference("flight L1 data"));
        node3.addXmlReference(new OutputTypeReference("flight L2 data"));
        node3.addXmlReference(new ModelTypeReference("Temperature references"));

        node.addNextNode(node3);

        assertEquals("node 2, node 3", node.getChildNodeNames());
        assertTrue(node.getNextNodes().contains(node3));
        assertTrue(node.getNextNodes().contains(node2));
    }

    private class NodeSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }
    }

    /**
     * Subclass of {@link PipelineNode} that allows an XmlRootElement annotation to be prepended.
     * This allows tests of the {@link PipelineNode} class as though it was a valid root element,
     * while not forcing the non-test use-cases to put up with the class being a root element.
     *
     * @author PT
     */
    @XmlRootElement(name = "node")
    @XmlAccessorType(XmlAccessType.NONE)
    private static class Node extends PipelineNode {

        @SuppressWarnings("unused")
        public Node() {
        }

        public Node(String pipelineStepName, String pipelineName) {
            super(pipelineStepName, pipelineName);
        }
    }
}
