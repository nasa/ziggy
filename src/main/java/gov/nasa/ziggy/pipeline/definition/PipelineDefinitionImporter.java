package gov.nasa.ziggy.pipeline.definition;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreOperations;
import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.module.ExternalProcessPipelineModule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineImportOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.pipeline.xml.XmlReference;
import gov.nasa.ziggy.uow.DatastoreDirectoryUnitOfWorkGenerator;
import gov.nasa.ziggy.util.ZiggyCollectionUtils;
import gov.nasa.ziggy.util.ZiggyStringUtils;

/**
 * Imports pipeline definitions from XML files.
 * <p>
 * When importing, the imported elements are compared to any existing elements with the same names.
 * If the existing elements are different, then they are updated to match the imported elements. A
 * 'dry run' report mode is available that indicates what elements would be changed with an import.
 * This report also acts as a 'diff' between two environments/clusters.
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineDefinitionImporter {
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinitionImporter.class);

    private ValidatingXmlManager<PipelineDefinitionFile> xmlManager;
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();
    private DatastoreOperations datastoreOperations = new DatastoreOperations();
    private ModelOperations modelOperations = new ModelOperations();
    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineImportOperations pipelineImportOperations = new PipelineImportOperations();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private boolean updateFlag;
    private boolean dryRun;

    public PipelineDefinitionImporter() {
        xmlManager = new ValidatingXmlManager<>(PipelineDefinitionFile.class);
    }

    /**
     * Imports the pipeline, module, and node definitions from a {@link Collection} of files in the
     * appropriate XML format.
     */
    public void importPipelineConfiguration(Collection<File> files) {
        for (File sourceFile : files) {
            if (!sourceFile.exists() || sourceFile.isDirectory()) {
                throw new IllegalArgumentException(
                    "sourcePath does not exist or is a directory: " + sourceFile);
            }
        }

        // Get the parameter set names from the database.
        // Load the content of all the files into appropriate Lists.
        List<PipelineDefinition> pipelineDefinitions = new ArrayList<>();
        List<PipelineModuleDefinition> pipelineModuleDefinitions = new ArrayList<>();
        for (File sourceFile : files) {
            log.info("Unmarshalling pipeline configuration from: " + sourceFile + "...");
            PipelineDefinitionFile pipelineDefinitionFile = xmlManager.unmarshal(sourceFile);
            pipelineDefinitions.addAll(pipelineDefinitionFile.getPipelines());
            pipelineModuleDefinitions.addAll(pipelineDefinitionFile.getModules());
            log.info("Unmarshalling pipeline configuration from: " + sourceFile + "...done");
        }
        List<String> pipelineModuleNames = pipelineModuleDefinitions.stream()
            .map(PipelineModuleDefinition::getName)
            .collect(Collectors.toList());

        log.debug("Importing Module Definitions...");
        Map<PipelineModuleDefinition, PipelineModuleExecutionResources> resourcesByModule = importModules(
            pipelineModuleDefinitions);
        log.debug("Importing Module Definitions...done");

        log.debug("Importing Pipeline Definitions");
        Map<PipelineDefinition, Set<PipelineDefinitionNodeExecutionResources>> resourcesByPipelineDefinition = importPipelines(
            pipelineDefinitions, pipelineModuleNames);
        log.debug("DONE importing pipeline configuration");
        if (!dryRun) {
            log.debug("Persisting module and pipeline definitions...");
            pipelineImportOperations().persistDefinitions(resourcesByModule,
                resourcesByPipelineDefinition);
            log.debug("Persisting module and pipeline definitions...done");
        }
    }

    private Map<PipelineModuleDefinition, PipelineModuleExecutionResources> importModules(
        List<PipelineModuleDefinition> newModules) {
        List<String> databaseModuleDefinitionNames = pipelineModuleDefinitionOperations()
            .allPipelineModuleDefinitions()
            .stream()
            .map(PipelineModuleDefinition::getName)
            .collect(Collectors.toList());
        Map<PipelineModuleDefinition, PipelineModuleExecutionResources> resourcesByModule = new HashMap<>();
        for (PipelineModuleDefinition newModule : newModules) {
            if (databaseModuleDefinitionNames.contains(newModule.getName())) {
                if (!updateFlag) {
                    log.warn("Module {} already present in database, not importing",
                        newModule.getName());
                    System.out.println("Module " + newModule.getName()
                        + " already present in database, not importing");
                    continue;
                }
                log.info("Updating definition of module {}", newModule.getName());
            } else {
                log.info("Creating new module {}", newModule.getName());
            }
            PipelineModuleExecutionResources executionResources = pipelineModuleDefinitionOperations()
                .pipelineModuleExecutionResources(newModule);
            executionResources.setExeTimeoutSeconds(newModule.getExeTimeoutSecs());
            executionResources.setMinMemoryMegabytes(newModule.getMinMemoryMegabytes());

            // Additional validation:
            // ExternalProcessPipelineModule must not have a UOW generator in its XML,
            // except for one that subclasses DatastoreDirectoryUnitOfWorkGenerator.
            // All other pipeline module classes must have a UOW generator in their XMLs.
            if (newModule.getPipelineModuleClass()
                .getClazz()
                .equals(ExternalProcessPipelineModule.class)) {
                if (newModule.getUnitOfWorkGenerator() == null) {
                    newModule.setUnitOfWorkGenerator(
                        new ClassWrapper<>(DatastoreDirectoryUnitOfWorkGenerator.class));
                }
                if (!DatastoreDirectoryUnitOfWorkGenerator.class
                    .isAssignableFrom(newModule.getUnitOfWorkGenerator().getClazz())) {
                    throw new PipelineException("Module " + newModule.getName()
                        + " uses ExternalProcessPipelineModule, specified UOW "
                        + newModule.getUnitOfWorkGenerator().getClazz().toString() + " is invalid");
                }
            } else if (newModule.getUnitOfWorkGenerator() == null) {
                throw new PipelineException(
                    "Module " + newModule.getName() + " must specify a unit of work generator");
            }
            resourcesByModule.put(newModule, executionResources);
        }
        return resourcesByModule;
    }

    private Map<PipelineDefinition, Set<PipelineDefinitionNodeExecutionResources>> importPipelines(
        List<PipelineDefinition> newPipelineDefinitions, List<String> importedModuleNames) {

        List<String> existingParameterSetNames = parametersOperations().parameterSetNames();
        List<String> databasePipelineDefinitionNames = pipelineDefinitionOperations()
            .allPipelineDefinitions()
            .stream()
            .map(PipelineDefinition::getName)
            .collect(Collectors.toList());
        Map<PipelineDefinition, Set<PipelineDefinitionNodeExecutionResources>> nodesByPipelineDefinition = new HashMap<>();
        for (PipelineDefinition newPipelineDefinition : newPipelineDefinitions) {

            String pipelineName = newPipelineDefinition.getName();
            if (databasePipelineDefinitionNames.contains(pipelineName)) {
                if (!updateFlag) {
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

            Set<PipelineDefinitionNode> nodes = newPipelineDefinition.nodesFromXml();
            Map<String, PipelineDefinitionNode> nodesByName = new HashMap<>();

            if (nodes.isEmpty()) {
                log.error("Pipeline " + pipelineName + " has no nodes");
            }
            for (PipelineDefinitionNode node : nodes) {
                String childNodeIds = node.getChildNodeNames() != null ? node.getChildNodeNames()
                    : "";
                log.info("Adding node " + node.getModuleName() + " for pipeline " + pipelineName
                    + " with child node(s) " + childNodeIds);
                nodesByName.put(node.getModuleName(), node);
            }

            String rootNodeString = newPipelineDefinition.getRootNodeNames();
            if (rootNodeString != null) {
                log.info("Pipeline " + pipelineName + " root node names: " + rootNodeString);
            } else {
                log.error("Pipeline " + pipelineName + " root node names string is null");
            }

            // pipeline-level parameters
            log.info("Parameter sets for pipeline " + pipelineName + ": "
                + newPipelineDefinition.parameterSetNamesFromXml().toString());
            Set<String> missingParameterSets = ZiggyCollectionUtils.elementsNotInSuperset(
                existingParameterSetNames, newPipelineDefinition.parameterSetNamesFromXml());
            if (!CollectionUtils.isEmpty(missingParameterSets)) {
                throw new PipelineException("Pipeline " + pipelineName
                    + " refers to missing parameter sets: " + missingParameterSets.toString());
            }
            newPipelineDefinition
                .setParameterSetNames(newPipelineDefinition.parameterSetNamesFromXml());

            Set<PipelineDefinitionNodeExecutionResources> nodeResources = new HashSet<>();
            List<String> rootNodeNames = ZiggyStringUtils
                .splitStringAtCommas(newPipelineDefinition.getRootNodeNames());
            addNodes(newPipelineDefinition.getName(), rootNodeNames,
                newPipelineDefinition.getRootNodes(), nodesByName, nodeResources,
                importedModuleNames, existingParameterSetNames);
            nodesByPipelineDefinition.put(newPipelineDefinition, nodeResources);
        }
        return nodesByPipelineDefinition;
    }

    private void addNodes(String pipelineName, List<String> rootNodeNames,
        List<PipelineDefinitionNode> pipelineRootNodes,
        Map<String, PipelineDefinitionNode> xmlNodesByName,
        Set<PipelineDefinitionNodeExecutionResources> nodeResources,
        List<String> importedModuleNames, List<String> existingParameterSetNames) {

        Map<String, DataFileType> dataFileTypeMap = datastoreOperations().dataFileTypeMap();
        Map<String, ModelType> modelTypeMap = modelOperations().modelTypeMap();

        for (String nodeName : rootNodeNames) {
            PipelineDefinitionNode xmlNode = xmlNodesByName.get(nodeName);
            if (xmlNode == null) {
                throw new PipelineException(
                    "No node found for root node " + nodeName + " in pipeline " + pipelineName);
            }
            PipelineModuleDefinition module = pipelineModuleDefinitionOperations()
                .pipelineModuleDefinition(xmlNode.getModuleName());
            if (module == null && !importedModuleNames.contains(xmlNode.getModuleName())) {
                throw new PipelineException("No module found for node " + xmlNode.getModuleName()
                    + " in pipeline " + pipelineName);
            }
            xmlNode.setPipelineName(pipelineName);

            // Module-level parameters
            log.info("Parameter sets for node " + nodeName + ": "
                + xmlNode.getXmlParameterSetNames().toString());
            Set<String> missingParameterSets = ZiggyCollectionUtils.elementsNotInSuperset(
                existingParameterSetNames, xmlNode.getXmlParameterSetNames());
            if (!CollectionUtils.isEmpty(missingParameterSets)) {
                throw new PipelineException("Module " + nodeName + " in pipeline " + pipelineName
                    + " refers to missing parameter sets: " + missingParameterSets.toString());
            }
            xmlNode.getParameterSetNames().addAll(xmlNode.getXmlParameterSetNames());

            // Data file types
            Set<String> missingDataFileTypes = new HashSet<>();
            Set<DataFileType> dataFileTypes = new HashSet<>();
            for (XmlReference xmlReference : xmlNode.getInputDataFileTypeReferences()) {
                DataFileType dataFileType = dataFileTypeMap.get(xmlReference.getName());
                if (dataFileType != null) {
                    dataFileTypes.add(dataFileType);
                } else {
                    missingDataFileTypes.add(xmlReference.getName());
                }
            }
            if (!missingDataFileTypes.isEmpty()) {
                throw new PipelineException(xmlNode.getModuleName()
                    + " missing input data file type names: " + missingDataFileTypes.toString());
            }
            xmlNode.addAllInputDataFileTypes(dataFileTypes);

            missingDataFileTypes = new HashSet<>();
            dataFileTypes = new HashSet<>();
            for (XmlReference xmlReference : xmlNode.getOutputDataFileTypeReferences()) {
                DataFileType dataFileType = dataFileTypeMap.get(xmlReference.getName());
                if (dataFileType != null) {
                    dataFileTypes.add(dataFileType);
                } else {
                    missingDataFileTypes.add(xmlReference.getName());
                }
            }
            if (!missingDataFileTypes.isEmpty()) {
                throw new PipelineException(xmlNode.getModuleName()
                    + " missing output data file type names: " + missingDataFileTypes.toString());
            }
            xmlNode.addAllOutputDataFileTypes(dataFileTypes);

            // model types
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
                throw new PipelineException(xmlNode.getModuleName() + " missing model type names: "
                    + missingModelTypes.toString());
            }
            xmlNode.addAllModelTypes(modelTypes);
            pipelineRootNodes.add(xmlNode);

            // Execution Resources: Store in an instance of
            // PipelineDefinitionNodeExecutionResources.
            PipelineDefinitionNodeExecutionResources executionResources = pipelineDefinitionNodeOperations()
                .pipelineDefinitionNodeExecutionResources(xmlNode);
            if (xmlNode.getHeapSizeMb() != null) {
                executionResources.setHeapSizeMb(xmlNode.getHeapSizeMb());
            }
            if (xmlNode.getMaxWorkerCount() != null) {
                executionResources.setMaxWorkerCount(xmlNode.getMaxWorkerCount());
            }
            nodeResources.add(executionResources);
            // Child nodes
            String childNodeIds = xmlNode.getChildNodeNames();

            if (!StringUtils.isBlank(childNodeIds)) {
                addNodes(pipelineName, ZiggyStringUtils.splitStringAtCommas(childNodeIds),
                    xmlNode.getNextNodes(), xmlNodesByName, nodeResources, importedModuleNames,
                    existingParameterSetNames);
            }
        }
    }

    public void setUpdate(boolean updateFlag) {
        this.updateFlag = updateFlag;
    }

    public void setDruRyn(boolean dryRun) {
        this.dryRun = dryRun;
    }

    PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }

    PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
    }

    DatastoreOperations datastoreOperations() {
        return datastoreOperations;
    }

    ModelOperations modelOperations() {
        return modelOperations;
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    PipelineImportOperations pipelineImportOperations() {
        return pipelineImportOperations;
    }

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }
}
