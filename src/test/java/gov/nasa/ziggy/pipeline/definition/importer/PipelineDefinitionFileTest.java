package gov.nasa.ziggy.pipeline.definition.importer;

import static gov.nasa.ziggy.XmlUtils.assertContains;
import static gov.nasa.ziggy.XmlUtils.complexTypeContent;
import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.DATASTORE_NODE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.DATASTORE_REGEXP;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.DATA_FILE_TYPE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.MODEL_TYPE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.PARAMETER_SET;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.PIPELINE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.PIPELINE_EVENT;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.REMOTE_ENVIRONMENT;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.STEP;
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

import gov.nasa.ziggy.XmlUtils;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
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
        xmlUnmarshalingFile = TEST_DATA.resolve("pd-hyperion.xml").toFile();
        schemaFile = directoryRule.directory().resolve("pipeline-file.xsd").toFile();
    }

    @Test
    public void testGenerateSchema() throws JAXBException, IOException {
        JAXBContext context = JAXBContext.newInstance(PipelineDefinitionFile.class);
        context.generateSchema(new PipelineFileSchemaResolver());
        List<String> schemaContent = Files.readAllLines(schemaFile.toPath(),
            ZiggyFileUtils.ZIGGY_CHARSET);

        assertContains("<xs:element name=\"pipelineDefinition\" type=\"pipelineDefinitionFile\"/>",
            schemaContent);

        checkPipelineDefinitionFile(schemaContent);
        checkPipelineStep(schemaContent);
        checkPipeline(schemaContent);
        checkPipelineNode(schemaContent);
        checkReferenceTypes(schemaContent);
        checkParameterSet(schemaContent);
        checkXmlParameter(schemaContent);
        checkDataFileType(schemaContent);
        checkModelType(schemaContent);
        checkDatastoreRegexp(schemaContent);
        checkDatastoreNode(schemaContent);
        checkZiggyEventHandler(schemaContent);
        checkRemoteEnvironment(schemaContent);
        checkArchitecture(schemaContent);
        checkBatchQueue(schemaContent);
    }

    /** Checks the content of the {@link PipelineDefinitionFile} complex type definition. */
    private void checkPipelineDefinitionFile(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineDefinitionFile\">");
        assertContains("<xs:element name=\"step\" type=\"pipelineStep\"/>", complexTypeContent);
        assertContains("<xs:element name=\"pipeline\" type=\"pipeline\"/>", complexTypeContent);
        assertContains("<xs:element name=\"parameterSet\" type=\"parameterSet\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"dataFileType\" type=\"dataFileType\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"modelType\" type=\"modelType\"/>", complexTypeContent);
        assertContains("<xs:element name=\"datastoreRegexp\" type=\"datastoreRegexp\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"datastoreNode\" type=\"datastoreNode\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"pipelineEvent\" type=\"ziggyEventHandler\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"remoteEnvironment\" type=\"remoteEnvironment\"/>",
            complexTypeContent);
        assertEquals(9, XmlUtils.elementCount(complexTypeContent));
        assertEquals(0, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link PipelineStep} complex type definition. */
    private void checkPipelineStep(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineStep\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"description\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"pipelineStepExecutorClass\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"inputsClass\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"outputsClass\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"exeTimeoutSecs\" type=\"xs:int\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"minMemoryMegabytes\" type=\"xs:int\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"uowGenerator\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"file\" type=\"xs:string\"/>", complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(9, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link Pipeline} complex type definition. */
    private void checkPipeline(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipeline\">");
        assertContains("<xs:element name=\"node\" type=\"pipelineNode\"/>", complexTypeContent);
        assertContains("<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"instancePriority\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"rootNodeNames\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"parameterSet\" type=\"parameterSetReference\"/>",
            complexTypeContent);
        assertEquals(2, XmlUtils.elementCount(complexTypeContent));
        assertEquals(4, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link PipelineNode} complex type definition. */
    private void checkPipelineNode(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"pipelineNode\">");
        assertContains("<xs:element name=\"parameterSet\" type=\"parameterSetReference\"/>",
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
        assertContains("<xs:attribute name=\"heapSizeMb\" type=\"xs:int\"/>", complexTypeContent);
        assertContains("<xs:attribute name=\"singleSubtask\" type=\"xs:boolean\"/>",
            complexTypeContent);
        assertEquals(4, XmlUtils.elementCount(complexTypeContent));
        assertEquals(5, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the reference type definitions. */
    private void checkReferenceTypes(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"inputTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(0, XmlUtils.attributeCount(complexTypeContent));

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"outputTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(0, XmlUtils.attributeCount(complexTypeContent));

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelTypeReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(0, XmlUtils.attributeCount(complexTypeContent));

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSetReference\">");
        assertContains("<xs:extension base=\"xmlReference\">", complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(0, XmlUtils.attributeCount(complexTypeContent));

        complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"xmlReference\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(1, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link ParameterSet} complex type definition. */
    private void checkParameterSet(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"parameterSet\">");
        assertContains(
            "<xs:element name=\"parameter\" type=\"xmlParameter\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"algorithmInterfaceName\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"partial\" type=\"xs:boolean\"/>", complexTypeContent);
        assertEquals(1, XmlUtils.elementCount(complexTypeContent));
        assertEquals(3, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link Parameter} complex type definition. */
    private void checkXmlParameter(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"xmlParameter\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"value\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"type\" type=\"xs:string\"/>", complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(3, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link DataFileType} complex type definition. */
    private void checkDataFileType(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"dataFileType\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"location\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"fileNameRegexp\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"includeAllFilesInAllSubtasks\" type=\"xs:boolean\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(4, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link ModelType} complex type definition. */
    private void checkModelType(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"modelType\">");
        assertContains("<xs:attribute name=\"type\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"fileNameRegex\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"versionNumberGroup\" type=\"xs:int\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"timestampGroup\" type=\"xs:int\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"semanticVersionNumber\" type=\"xs:boolean\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(5, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link DatastoreRegexp} complex type definition. */
    private void checkDatastoreRegexp(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"datastoreRegexp\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"value\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(2, XmlUtils.attributeCount(complexTypeContent));
    }

    /** Checks the content of the {@link DatastoreNode} complex type definition. */
    private void checkDatastoreNode(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"datastoreNode\">");
        assertContains(
            "<xs:element name=\"datastoreNode\" type=\"datastoreNode\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"isRegexp\" type=\"xs:boolean\"/>", complexTypeContent);
        assertContains("<xs:attribute name=\"nodes\" type=\"xs:string\"/>", complexTypeContent);
        assertEquals(1, XmlUtils.elementCount(complexTypeContent));
        assertEquals(3, XmlUtils.attributeCount(complexTypeContent));
    }

    private void checkZiggyEventHandler(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"ziggyEventHandler\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains(
            "<xs:attribute name=\"enableOnClusterStart\" type=\"xs:boolean\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"directory\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"pipelineName\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(4, XmlUtils.attributeCount(complexTypeContent));
    }

    private void checkRemoteEnvironment(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"remoteEnvironment\">");
        assertContains("<xs:element name=\"architecture\" type=\"architecture\"/>",
            complexTypeContent);
        assertContains("<xs:element name=\"queue\" type=\"batchQueue\"/>", complexTypeContent);
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"queueTimeMetricsClass\" type=\"xs:string\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"costUnit\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"batchSystem\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertEquals(2, XmlUtils.elementCount(complexTypeContent));
        assertEquals(5, XmlUtils.attributeCount(complexTypeContent));
    }

    private void checkArchitecture(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"architecture\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"cores\" type=\"xs:int\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"ramGigabytes\" type=\"xs:int\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"cost\" type=\"xs:float\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"bandwidthGbps\" type=\"xs:float\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"nodeCollectionNamesFile\" type=\"xs:string\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(7, XmlUtils.attributeCount(complexTypeContent));
    }

    private void checkBatchQueue(List<String> schemaContent) {
        List<String> complexTypeContent = complexTypeContent(schemaContent,
            "<xs:complexType name=\"batchQueue\">");
        assertContains("<xs:attribute name=\"name\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"description\" type=\"xs:string\" use=\"required\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"maxWallTimeHours\" type=\"xs:float\"/>",
            complexTypeContent);
        assertContains("<xs:attribute name=\"maxNodes\" type=\"xs:int\"/>", complexTypeContent);
        assertContains("<xs:attribute name=\"reserved\" type=\"xs:boolean\"/>", complexTypeContent);
        assertContains("<xs:attribute name=\"autoSelectable\" type=\"xs:boolean\"/>",
            complexTypeContent);
        assertEquals(0, XmlUtils.elementCount(complexTypeContent));
        assertEquals(6, XmlUtils.attributeCount(complexTypeContent));
    }

    @Test
    public void testUnmarshaller() throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(PipelineDefinitionFile.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        PipelineDefinitionFile pipelineDefinitionFile = (PipelineDefinitionFile) unmarshaller
            .unmarshal(xmlUnmarshalingFile);
        assertEquals(1, pipelineDefinitionFile.getPipelineElements(PIPELINE).size());
        assertEquals(2, pipelineDefinitionFile.getPipelineElements(STEP).size());
        assertEquals(2, pipelineDefinitionFile.getPipelineElements(DATASTORE_REGEXP).size());
        List<DatastoreNode> datastoreNodes = pipelineDefinitionFile
            .getPipelineElements(DATASTORE_NODE);
        assertEquals(1, datastoreNodes.size());
        DatastoreNode datastoreNode = datastoreNodes.get(0);
        assertEquals(5, datastoreNode.getXmlNodes().size());
        assertEquals(3, pipelineDefinitionFile.getPipelineElements(DATA_FILE_TYPE).size());
        assertEquals(10, pipelineDefinitionFile.getPipelineElements(MODEL_TYPE).size());
        assertEquals(5, pipelineDefinitionFile.getPipelineElements(PARAMETER_SET).size());
        assertEquals(3, pipelineDefinitionFile.getPipelineElements(PIPELINE_EVENT).size());
        assertEquals(29, pipelineDefinitionFile.getPipelineElements().size());
        assertEquals(2, pipelineDefinitionFile.getPipelineElements(REMOTE_ENVIRONMENT).size());
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
