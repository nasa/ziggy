package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.pipeline.definition.XmlUtils.assertContains;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.compareParameterSetReferences;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.compareXmlReferences;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.nodeContent;
import static gov.nasa.ziggy.pipeline.definition.XmlUtils.pipelineContent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.parameters.DefaultParameters;
import gov.nasa.ziggy.pipeline.xml.XmlReference.InputTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.ModelTypeReference;
import gov.nasa.ziggy.pipeline.xml.XmlReference.OutputTypeReference;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.util.io.Filenames;

/**
 * Unit tests for {@link PipelineDefinition} class. These tests exercise the XML interface, since
 * most of the rest of the class is getters and setters.
 *
 * @author PT
 */
public class PipelineDefinitionTest {

    private PipelineDef pipelineDefinition1;
    private String workingDirName;
    private File xmlFile;
    private File xmlUnmarshalingFile;
    private File schemaFile;

    @Before
    public void setUp() {

        workingDirName = System.getProperty("user.dir");
        xmlUnmarshalingFile = new File(
            workingDirName + "/test/data/configuration/pipeline-definition.xml");
        String workingDir = workingDirName + "/build/test";
        new File(workingDir).mkdirs();
        xmlFile = new File(workingDir, "pipeline-definition.xml");
        schemaFile = new File(workingDir, "pipeline-definition.xsd");

        // Create some nodes
        PipelineDefinitionNode node1 = new PipelineDefinitionNode(new ModuleName("module 1"), null);
        node1.setStartNewUow(true);
        node1.setUnitOfWorkGenerator(new ClassWrapper<>(SingleUnitOfWorkGenerator.class));
        node1.addXmlReference(new ParameterSetName("Remote execution"));
        node1.addXmlReference(new ParameterSetName("Convergence criteria"));
        node1.addXmlReference(new InputTypeReference("flight L0 data"));
        node1.addXmlReference(new OutputTypeReference("flight L1 data"));
        node1.addXmlReference(new ModelTypeReference("calibration constants"));

        PipelineDefinitionNode node2 = new PipelineDefinitionNode(new ModuleName("module 2"), null);
        node2.setStartNewUow(true);
        node2.addXmlReference(new ParameterSetName("Remote execution"));
        node2.addXmlReference(new ParameterSetName("Convergence criteria"));
        node2.addXmlReference(new InputTypeReference("flight L1 data"));
        node2.addXmlReference(new OutputTypeReference("flight L2 data"));
        node2.addXmlReference(new ModelTypeReference("georeferencing constants"));

        PipelineDefinitionNode node3 = new PipelineDefinitionNode(new ModuleName("module 3"), null);
        node3.setStartNewUow(true);
        node3.addXmlReference(new ParameterSetName("Excluded bands"));
        node3.addXmlReference(new InputTypeReference("flight L1 data"));
        node3.addXmlReference(new OutputTypeReference("flight L2 data"));
        node3.addXmlReference(new ModelTypeReference("Temperature references"));

        node1.addNextNode(node2);
        node1.addNextNode(node3);

        PipelineDefinitionNode node4 = new PipelineDefinitionNode(new ModuleName("module 4"), null);
        node4.setStartNewUow(true);
        node4.addXmlReference(new ParameterSetName("Export format"));
        node4.addXmlReference(new InputTypeReference("flight L2 data"));
        node4.addXmlReference(new OutputTypeReference("exports"));

        node2.addNextNode(node4);

        // Create 2 pipelines
        pipelineDefinition1 = new PipelineDef("pipeline 1");
        pipelineDefinition1.addRootNode(node1);
        pipelineDefinition1.setDescription("first pipeline");
        pipelineDefinition1.addPipelineParameterSetName(DefaultParameters.class,
            new ParameterSet("Pipeline parameters"));
        pipelineDefinition1.setInstancePriority(2);

    }

    @After
    public void tearDown() throws IOException {
        xmlFile.delete();
        schemaFile.delete();
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test
    public void testMarshaller() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineDef.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.marshal(pipelineDefinition1, xmlFile);
        assertTrue(xmlFile.exists());
        List<String> xmlContent = Files.readAllLines(xmlFile.toPath());

        List<String> pipelineContents = pipelineContent(xmlContent,
            "<pipeline description=\"first pipeline\" name=\"pipeline 1\" "
                + "instancePriority=\"2\" rootNodeNames=\"module 1\">");
        assertContains(pipelineContents, "<pipelineParameter name=\"Pipeline parameters\"/>");
        List<String> nodeContents = nodeContent(pipelineContents,
            "<node startNewUow=\"true\" uowGenerator=\"gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator\" "
                + "moduleName=\"module 1\" childNodeNames=\"module 2, module 3\">");
        assertContains(nodeContents, "<moduleParameter name=\"Convergence criteria\"/>");
        assertContains(nodeContents, "<moduleParameter name=\"Remote execution\"/>");
        assertContains(nodeContents, "<inputDataFileType name=\"flight L0 data\"/>");
        assertContains(nodeContents, "<outputDataFileType name=\"flight L1 data\"/>");
        nodeContents = nodeContent(pipelineContents,
            "<node startNewUow=\"true\" " + "moduleName=\"module 2\" childNodeNames=\"module 4\">");
        assertContains(nodeContents, "<moduleParameter name=\"Convergence criteria\"/>");
        assertContains(nodeContents, "<moduleParameter name=\"Remote execution\"/>");
        assertContains(nodeContents, "<inputDataFileType name=\"flight L1 data\"/>");
        assertContains(nodeContents, "<outputDataFileType name=\"flight L2 data\"/>");
        assertContains(nodeContents, "<modelType name=\"georeferencing constants\"/>");
        nodeContents = nodeContent(pipelineContents,
            "<node startNewUow=\"true\" moduleName=\"module 3\">");
        assertContains(nodeContents, "<moduleParameter name=\"Excluded bands\"/>");
        assertContains(nodeContents, "<inputDataFileType name=\"flight L1 data\"/>");
        assertContains(nodeContents, "<outputDataFileType name=\"flight L2 data\"/>");
        assertContains(nodeContents, "<modelType name=\"Temperature references\"/>");
        nodeContents = nodeContent(pipelineContents,
            "<node startNewUow=\"true\" moduleName=\"module 4\">");
        assertContains(nodeContents, "<moduleParameter name=\"Export format\"/>");
        assertContains(nodeContents, "<inputDataFileType name=\"flight L2 data\"/>");
        assertContains(nodeContents, "<outputDataFileType name=\"exports\"/>");

    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(PipelineDef.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        PipelineDefinition pipelineDefinition = (PipelineDef) unmarshaller
            .unmarshal(xmlUnmarshalingFile);
        comparePipelines(pipelineDefinition1, pipelineDefinition);
    }

    private void comparePipelines(PipelineDefinition groundTruthPipeline,
        PipelineDefinition pipeline) {
        assertEquals(groundTruthPipeline.getDescription(), pipeline.getDescription());
        assertEquals(groundTruthPipeline.getInstancePriority(), pipeline.getInstancePriority());
        assertEquals(groundTruthPipeline.getRootNodeNames(), pipeline.getRootNodeNames());
        assertEquals(groundTruthPipeline.getParameterSetNames().size(),
            pipeline.getParameterSetNames().size());
        for (ParameterSetName paramSetName : pipeline.getParameterSetNames()) {
            assertTrue(groundTruthPipeline.getParameterSetNames().contains(paramSetName));
        }
        for (PipelineDefinitionNode node : pipeline.getNodes()) {
            checkNode(node, groundTruthPipeline.getNodes());
        }
    }

    private void checkNode(PipelineDefinitionNode node,
        Collection<PipelineDefinitionNode> groundTruthNodes) {
        PipelineDefinitionNode groundTruthNode = null;
        for (PipelineDefinitionNode loopNode : groundTruthNodes) {
            if (loopNode.getModuleName().getName().equals(node.getModuleName().getName())) {
                groundTruthNode = loopNode;
                break;
            }
        }
        assertFalse(groundTruthNode == null);
        assertEquals(groundTruthNode.isStartNewUow(), node.isStartNewUow());
        assertEquals(groundTruthNode.getUnitOfWorkGenerator(), node.getUnitOfWorkGenerator());
        assertEquals(groundTruthNode.getChildNodeNames(), node.getChildNodeNames());
        compareXmlReferences(groundTruthNode.getModelTypeReferences(),
            node.getModelTypeReferences());
        compareXmlReferences(groundTruthNode.getInputDataFileTypeReferences(),
            node.getInputDataFileTypeReferences());
        compareXmlReferences(groundTruthNode.getOutputDataFileTypeReferences(),
            node.getOutputDataFileTypeReferences());
        compareParameterSetReferences(groundTruthNode.getParameterSetNames(),
            node.getParameterSetNames());

    }

    @Test
    public void testGenerateSchema() throws IOException, JAXBException {
        JAXBContext context = JAXBContext.newInstance(PipelineDef.class);
        context.generateSchema(new PipelineDefinitionListSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath());
        assertContains(schemaContent, "<xs:element name=\"pipeline\" type=\"pipelineDef\"/>");

        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinition\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"node\" type=\"pipelineDefinitionNode\"/>");
        assertContains(complexTypeContent,
            "<xs:element name=\"pipelineParameter\" " + "type=\"parameterSetName\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"instancePriority\" type=\"xs:int\"/>");
        assertContains(complexTypeContent,
            "<xs:attribute name=\"rootNodeNames\" type=\"xs:string\" use=\"required\"/>");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetName\">");
        assertContains(complexTypeContent, "<xs:extension base=\"xmlReference\">");

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinitionNode\">");
        assertContains(complexTypeContent,
            "<xs:element name=\"moduleParameter\" " + "type=\"parameterSetName\"/>");
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

    private class PipelineDefinitionListSchemaResolver extends SchemaOutputResolver {

        @Override
        public Result createOutput(String namespaceUri, String suggestedFileName)
            throws IOException {
            StreamResult result = new StreamResult(schemaFile);
            result.setSystemId(schemaFile.toURI().toURL().toString());
            return result;
        }
    }

    /**
     * Subclass of {@link PipelineDefinition} that allows an XmlRootElement annotation to be
     * prepended. This allows tests of the {@link PipelineDefinition} class as though it was a valid
     * root element, while not forcing the non-test use-cases to put up with the class being a root
     * element.
     *
     * @author PT
     */
    @XmlRootElement(name = "pipeline")
    @XmlAccessorType(XmlAccessType.NONE)
    private static class PipelineDef extends PipelineDefinition {

        public PipelineDef() {

        }

        public PipelineDef(String name) {
            super(name);
        }
    }

}
