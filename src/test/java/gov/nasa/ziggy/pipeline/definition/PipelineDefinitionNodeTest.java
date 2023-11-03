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
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
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
 * Unit tests for {@link PipelineDefinitionNode} class. These are primarily tests of the XML
 * conversion system, since the rest of the class is just getters and setters.
 *
 * @author PT
 */
public class PipelineDefinitionNodeTest {

    private Node node;
    private File xmlFile;
    private File xmlUnmarshalingFile;
    private File schemaFile;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setUp() {

        // Set the working directory
        xmlUnmarshalingFile = TEST_DATA.resolve("configuration").resolve("node.xml").toFile();
        xmlFile = directoryRule.directory().resolve("node.xml").toFile();
        schemaFile = directoryRule.directory().resolve("node.xsd").toFile();

        // Construct a new node for the test
        node = new Node("module 1", null);
        node.setStartNewUow(true);
        node.setUnitOfWorkGenerator(new ClassWrapper<>(SingleUnitOfWorkGenerator.class));
        node.setChildNodeNames("module 2, module 3");
        Set<XmlReference> xmlReferences = new HashSet<>();
        xmlReferences.add(new ParameterSetReference("Remote execution"));
        xmlReferences.add(new ParameterSetReference("Convergence criteria"));
        xmlReferences.add(new InputTypeReference("flight L0 data"));
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
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath(), FileUtil.ZIGGY_CHARSET);
        assertEquals(8, xmlContent.size());
        List<String> nodeContent = nodeContent(xmlContent,
            "<node startNewUow=\"true\" " + "maxWorkers=\"0\" " + "heapSizeMb=\"0\" "
                + "uowGenerator=\"gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator\" "
                + "moduleName=\"module 1\" childNodeNames=\"module 2, module 3\">");
        String[] xmlLines = { "<moduleParameter name=\"Convergence criteria\"/>",
            "<moduleParameter name=\"Remote execution\"/>",
            "<inputDataFileType name=\"flight L0 data\"/>",
            "<outputDataFileType name=\"flight L1 data\"/>",
            "<modelType name=\"calibration constants\"/>" };
        for (String xmlLine : xmlLines) {
            assertContains(nodeContent, xmlLine);
        }
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(Node.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        PipelineDefinitionNode node = (Node) unmarshaller.unmarshal(xmlUnmarshalingFile);
        assertEquals("module 1", node.getModuleName());
        assertTrue(node.isStartNewUow());
        assertEquals("module 2, module 3", node.getChildNodeNames());
        assertEquals(2, node.getParameterSetNames().size());
        assertTrue(node.getParameterSetNames().contains("Remote execution"));
        assertTrue(node.getParameterSetNames().contains("Convergence criteria"));
        assertEquals(1, node.getInputDataFileTypeReferences().size());
        assertTrue(node.getInputDataFileTypeReferences()
            .contains(new InputTypeReference("flight L0 data")));
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
            FileUtil.ZIGGY_CHARSET);
        assertContains(schemaContent, "<xs:element name=\"node\" type=\"node\"/>");

        List<String> nodeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"node\">");
        assertContains(nodeContent, "<xs:extension base=\"pipelineDefinitionNode\">");

        nodeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinitionNode\">");
        String[] nodeStrings = {
            "<xs:element name=\"moduleParameter\" type=\"parameterSetReference\"/>",
            "<xs:element name=\"inputDataFileType\" type=\"inputTypeReference\"/>",
            "<xs:element name=\"outputDataFileType\" type=\"outputTypeReference\"/>",
            "<xs:element name=\"modelType\" type=\"modelTypeReference\"/>",
            "<xs:attribute name=\"startNewUow\" type=\"xs:boolean\"/>",
            "<xs:attribute name=\"uowGenerator\" type=\"xs:string\"/>",
            "<xs:attribute name=\"maxWorkers\" type=\"xs:int\"/>",
            "<xs:attribute name=\"moduleName\" type=\"xs:string\" use=\"required\"/>",
            "<xs:attribute name=\"childNodeNames\" type=\"xs:string\"/>" };
        for (String nodeString : nodeStrings) {
            assertContains(nodeContent, nodeString);
        }

        List<String> xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"xmlReference\">");
        assertContains(xmlReferenceContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"inputTypeReference\">");
        assertContains(xmlReferenceContent, "<xs:extension base=\"xmlReference\">");

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"outputTypeReference\">");
        assertContains(xmlReferenceContent, "<xs:extension base=\"xmlReference\">");

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelTypeReference\">");
        assertContains(xmlReferenceContent, "<xs:extension base=\"xmlReference\">");

        xmlReferenceContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetReference\">");
        assertContains(xmlReferenceContent, "<xs:extension base=\"xmlReference\">");
    }

    @Test
    public void testNodeListConstruction() {

        PipelineDefinitionNode node2 = new PipelineDefinitionNode("module 2", null);
        node2.setStartNewUow(true);
        node2.addXmlReference(new ParameterSetReference("Remote execution"));
        node2.addXmlReference(new ParameterSetReference("Convergence criteria"));
        node2.addXmlReference(new InputTypeReference("flight L1 data"));
        node2.addXmlReference(new OutputTypeReference("flight L2 data"));
        node2.addXmlReference(new ModelTypeReference("georeferencing constants"));

        node.addNextNode(node2);

        PipelineDefinitionNode node3 = new PipelineDefinitionNode("module 3", null);
        node3.setStartNewUow(true);
        node3.addXmlReference(new ParameterSetReference("Excluded bands"));
        node3.addXmlReference(new InputTypeReference("flight L1 data"));
        node3.addXmlReference(new OutputTypeReference("flight L2 data"));
        node3.addXmlReference(new ModelTypeReference("Temperature references"));

        node.addNextNode(node3);

        assertEquals("module 2, module 3", node.getChildNodeNames());
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
     * Subclass of {@link PipelineDefinitionNode} that allows an XmlRootElement annotation to be
     * prepended. This allows tests of the {@link PipelineDefinitionNode} class as though it was a
     * valid root element, while not forcing the non-test use-cases to put up with the class being a
     * root element.
     *
     * @author PT
     */
    @XmlRootElement(name = "node")
    @XmlAccessorType(XmlAccessType.NONE)
    private static class Node extends PipelineDefinitionNode {

        @SuppressWarnings("unused")
        public Node() {
        }

        public Node(String pipelineModuleDefinitionName, String pipelineDefinitionName) {
            super(pipelineModuleDefinitionName, pipelineDefinitionName);
        }
    }
}
