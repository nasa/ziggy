package gov.nasa.ziggy.pipeline.definition.importer;

import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.data.management.DataReceiptPipelineStepExecutor;
import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;
import gov.nasa.ziggy.pipeline.xml.XmlReference;
import gov.nasa.ziggy.util.PipelineException;
import gov.nasa.ziggy.util.ZiggyCollectionUtils;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Performs conditioning of imported pipelines to a state that can be persisted to the database.
 * <p>
 * Conditioning of the imports consists of the following:
 * <ol>
 * <li>Rejection of any update imports if the update flag is not set.
 * <li>Performing updates of existing pipelines if the update flag is set.
 * <li>Conversion of the tree of pipeline nodes from Strings to {@link PipelineNode} instances.
 * <li>Conversion of data file type and model types from String references to actual
 * {@link DataFileType} and {@link ModelType} instances.
 * <li>Checking for parameter set references that are missing from the database.
 * <li>Checking for pipeline nodes that do not have a corresponding {@link PipelineStep}.
 * <li>Checking for assorted error conditions (pipeline with no nodes, nodes with no names, etc).
 * </ol>
 *
 * @author PT
 */
public class PipelineImportConditioner {

    private static final Logger log = LoggerFactory.getLogger(PipelineImportConditioner.class);

    private PipelineOperations pipelineOperations = new PipelineOperations();
    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();
    private DatastoreOperations datastoreOperations = new DatastoreOperations();
    private ModelOperations modelOperations = new ModelOperations();
    private final List<String> parameterSetNames;
    private final boolean update;
    private final List<DataFileType> importedDataFileTypes;
    private final List<ModelType> importedModelTypes;
    private final List<PipelineStep> importedPipelineSteps;
    private Set<String> pipelineStepNames;

    public PipelineImportConditioner(List<PipelineStep> importedPipelineSteps,
        List<ParameterSetDescriptor> parameterSetDescriptors,
        List<DataFileType> importedDataFileTypes, List<ModelType> importedModelTypes,
        boolean update) {
        this.update = update;
        parameterSetNames = parameterSetDescriptors.stream()
            .map(ParameterSetDescriptor::getName)
            .toList();
        this.importedDataFileTypes = importedDataFileTypes;
        this.importedModelTypes = importedModelTypes;
        this.importedPipelineSteps = importedPipelineSteps;
    }

    public Map<Pipeline, Set<PipelineNodeExecutionResources>> importPipelines(
        List<Pipeline> newPipelines) {

        List<String> databasePipelineNames = pipelineOperations().allPipelines()
            .stream()
            .map(Pipeline::getName)
            .collect(Collectors.toList());
        pipelineStepNames = pipelineStepNames();
        Map<Pipeline, Set<PipelineNodeExecutionResources>> nodesByPipeline = new HashMap<>();
        for (Pipeline newPipeline : newPipelines) {

            String pipelineName = newPipeline.getName();
            if (databasePipelineNames.contains(pipelineName)) {
                if (!update) {
                    log.warn("Pipeline {} already present in database, not importing",
                        pipelineName);
                    System.out.println(
                        "Pipeline " + pipelineName + " already present in database, not importing");
                    continue;
                }
                log.info("Updating definition of pipeline {}", pipelineName);
            } else {
                log.info("Creating new pipeline defintion {}", pipelineName);
            }

            Set<PipelineNode> nodes = newPipeline.nodesFromXml();
            Map<String, PipelineNode> nodesByName = new HashMap<>();

            if (nodes.isEmpty()) {
                log.error("Pipeline {} has no nodes", pipelineName);
            }
            for (PipelineNode node : nodes) {
                String childNodeIds = node.getChildNodeNames() != null ? node.getChildNodeNames()
                    : "";
                log.info("Adding node {} for pipeline {} with child node(s) {}",
                    node.getPipelineStepName(), pipelineName, childNodeIds);
                nodesByName.put(node.getPipelineStepName(), node);
            }

            String rootNodeString = newPipeline.getRootNodeNames();
            if (rootNodeString != null) {
                log.info("Pipeline {} root node names are {}", pipelineName, rootNodeString);
            } else {
                log.error("Pipeline {} root node names string is null", pipelineName);
            }

            // pipeline-level parameters
            log.info("Parameter sets for pipeline {} are {}", pipelineName,
                newPipeline.parameterSetNamesFromXml().toString());
            Set<String> missingParameterSets = ZiggyCollectionUtils
                .elementsNotInSuperset(parameterSetNames, newPipeline.parameterSetNamesFromXml());
            if (!CollectionUtils.isEmpty(missingParameterSets)) {
                throw new PipelineException("Pipeline " + pipelineName
                    + " refers to missing parameter sets: " + missingParameterSets.toString());
            }
            newPipeline.setParameterSetNames(newPipeline.parameterSetNamesFromXml());

            Set<PipelineNodeExecutionResources> nodeResources = new HashSet<>();
            List<String> rootNodeNames = ZiggyStringUtils
                .splitStringAtCommas(newPipeline.getRootNodeNames());
            addNodes(newPipeline.getName(), rootNodeNames, newPipeline.getRootNodes(), nodesByName,
                nodeResources);
            nodesByPipeline.put(newPipeline, nodeResources);
        }
        return nodesByPipeline;
    }

    Set<String> pipelineStepNames() {
        Set<String> names = importedPipelineSteps.stream()
            .map(PipelineStep::getName)
            .collect(Collectors.toSet());
        List<String> databaseNames = pipelineStepOperations().pipelineSteps()
            .stream()
            .map(PipelineStep::getName)
            .toList();
        names.addAll(databaseNames);
        return names;
    }

    private void addNodes(String pipelineName, List<String> rootNodeNames,
        List<PipelineNode> pipelineRootNodes, Map<String, PipelineNode> xmlNodesByName,
        Set<PipelineNodeExecutionResources> nodeResources) {

        for (String nodeName : rootNodeNames) {
            PipelineNode xmlNode = xmlNodesByName.get(nodeName);
            if (xmlNode == null) {
                throw new PipelineException(
                    "No node found for root node " + nodeName + " in pipeline " + pipelineName);
            }
            PipelineStep pipelineStep = pipelineStepOperations()
                .pipelineStep(xmlNode.getPipelineStepName());
            if (pipelineStep == null
                && !pipelineStepNames.contains(xmlNode.getPipelineStepName())) {
                throw new PipelineException("No pipeline step found for node "
                    + xmlNode.getPipelineStepName() + " in pipeline " + pipelineName);
            }
            xmlNode.setPipelineName(pipelineName);

            // Special case: the user is not required to know that data receipt nodes are
            // always single-subtask.
            if (xmlNode.getPipelineStepName()
                .equals(DataReceiptPipelineStepExecutor.DATA_RECEIPT_PIPELINE_STEP_EXECUTOR_NAME)) {
                xmlNode.setSingleSubtask(true);
            }

            // Node-level parameters
            setNodeParameterSetNames(xmlNode);

            // Data file types
            Map<String, DataFileType> dataFileTypeByName = dataFileTypeByName();
            xmlNode.addAllInputDataFileTypes(nodeDataFileTypes(xmlNode.getPipelineStepName(),
                xmlNode.getInputDataFileTypeReferences(), dataFileTypeByName, "input"));
            xmlNode.addAllOutputDataFileTypes(nodeDataFileTypes(xmlNode.getPipelineStepName(),
                xmlNode.getOutputDataFileTypeReferences(), dataFileTypeByName, "output"));

            // Model types
            xmlNode.addAllModelTypes(nodeModelTypes(xmlNode));

            pipelineRootNodes.add(xmlNode);

            // Execution Resources: Store in an instance of
            // PipelineNodeExecutionResources.
            PipelineNodeExecutionResources executionResources = pipelineNodeOperations()
                .pipelineNodeExecutionResources(xmlNode);
            if (xmlNode.getHeapSizeGigabytes() != null) {
                executionResources.setHeapSizeGigabytes(xmlNode.getHeapSizeGigabytes());
            }
            if (xmlNode.getMaxWorkerCount() != null) {
                executionResources.setMaxWorkerCount(xmlNode.getMaxWorkerCount());
            }
            nodeResources.add(executionResources);
            // Child nodes
            String childNodeIds = xmlNode.getChildNodeNames();

            if (!StringUtils.isBlank(childNodeIds)) {
                addNodes(pipelineName, ZiggyStringUtils.splitStringAtCommas(childNodeIds),
                    xmlNode.getNextNodes(), xmlNodesByName, nodeResources);
            }
        }
    }

    private void setNodeParameterSetNames(PipelineNode xmlNode) {
        String nodeName = xmlNode.getPipelineStepName();
        log.info("Parameter sets for node {} are {}", nodeName,
            xmlNode.getXmlParameterSetNames().toString());
        Set<String> missingParameterSets = ZiggyCollectionUtils
            .elementsNotInSuperset(parameterSetNames, xmlNode.getXmlParameterSetNames());
        if (!CollectionUtils.isEmpty(missingParameterSets)) {
            throw new PipelineException(
                "Node " + nodeName + " in pipeline " + xmlNode.getPipelineName()
                    + " refers to missing parameter sets: " + missingParameterSets.toString());
        }
        xmlNode.getParameterSetNames().addAll(xmlNode.getXmlParameterSetNames());
    }

    Map<String, DataFileType> dataFileTypeByName() {
        Map<String, DataFileType> dataFileTypeByName = importedDataFileTypes.stream()
            .collect(toMap(DataFileType::getName, t -> t));

        dataFileTypeByName.putAll(datastoreOperations().dataFileTypeMap());
        return dataFileTypeByName;
    }

    private Set<DataFileType> nodeDataFileTypes(String nodeName,
        Set<? extends XmlReference> dataFileTypeReferences,
        Map<String, DataFileType> dataFileTypeByName, String inputOutput) {
        Set<String> missingDataFileTypes = new HashSet<>();
        Set<DataFileType> dataFileTypes = new HashSet<>();
        for (XmlReference xmlReference : dataFileTypeReferences) {
            DataFileType dataFileType = dataFileTypeByName.get(xmlReference.getName());
            if (dataFileType != null) {
                dataFileTypes.add(dataFileType);
            } else {
                missingDataFileTypes.add(xmlReference.getName());
            }
        }
        if (!missingDataFileTypes.isEmpty()) {
            throw new PipelineException(nodeName + " missing " + inputOutput
                + " data file type names: " + missingDataFileTypes.toString());
        }
        return dataFileTypes;
    }

    private Set<ModelType> nodeModelTypes(PipelineNode xmlNode) {
        Map<String, ModelType> modelTypeMap = modelTypesByName();
        Set<String> missingModelTypes = new HashSet<>();
        Set<ModelType> modelTypes = new HashSet<>();
        for (XmlReference xmlReference : xmlNode.getModelTypeReferences()) {
            ModelType modelType = modelTypeMap.get(xmlReference.getName());
            if (modelType != null) {
                modelTypes.add(modelType);
            } else {
                missingModelTypes.add(xmlReference.getName());
            }
        }
        if (!missingModelTypes.isEmpty()) {
            throw new PipelineException(xmlNode.getPipelineStepName()
                + " missing model type names: " + missingModelTypes.toString());
        }
        return modelTypes;
    }

    Map<String, ModelType> modelTypesByName() {
        Map<String, ModelType> modelTypesByName = new HashMap<>();
        for (ModelType modelType : importedModelTypes) {
            modelTypesByName.put(modelType.getType(), modelType);
        }
        modelTypesByName.putAll(modelOperations().modelTypeMap());
        return modelTypesByName;
    }

    ModelOperations modelOperations() {
        return modelOperations;
    }

    PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }

    PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }

    PipelineNodeOperations pipelineNodeOperations() {
        return pipelineNodeOperations;
    }

    DatastoreOperations datastoreOperations() {
        return datastoreOperations;
    }
}
