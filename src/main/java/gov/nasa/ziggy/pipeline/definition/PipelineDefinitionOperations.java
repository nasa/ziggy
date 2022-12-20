package gov.nasa.ziggy.pipeline.definition;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.data.management.DataFileType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.crud.DataFileTypeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.pipeline.xml.XmlReference;
import jakarta.xml.bind.JAXBException;

/**
 * Contains methods for importing and exporting pipeline configurations.
 * <p>
 * For export, a List&lt;PipelineDefinition&gt; is specified. All of the pipeline data is exported,
 * as well as the module definitions referenced by the pipelines. The XML file references parameter
 * sets by name, but does NOT contain the contents of the parameter sets themselves. The parameter
 * library import/export is in a separate XML file (see {@link ParametersOperations}
 * <p>
 * When importing, the imported elements are compared to any existing elements with the same names.
 * If the existing elements are different, then they are updated to match the imported elements. A
 * 'dry run' report mode is available that indicates what elements would be changed with an import.
 * This report also acts as a 'diff' between two environments/clusters.
 *
 * @author Todd Klaus
 * @author PT
 */
public class PipelineDefinitionOperations {
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinitionOperations.class);

    private PipelineModuleDefinitionCrud moduleCrud;
    private PipelineDefinitionCrud pipelineCrud;
    private ParameterSetCrud parameterSetCrud;
    private PipelineOperations pipelineOperations;
    private DataFileTypeCrud dataFileTypeCrud;
    private ModelCrud modelCrud;
    private ValidatingXmlManager<PipelineDefinitionFile> xmlManager;

    public PipelineDefinitionOperations() {
        try {
            xmlManager = new ValidatingXmlManager<>(PipelineDefinitionFile.class);
        } catch (InstantiationException | IllegalAccessException | SAXException | JAXBException
            | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
            | SecurityException e) {
            throw new PipelineException(
                "Unable to construct ValidatingXmlManager for class PipelineDefinitionFile", e);
        }
    }

    /**
     * Export the specified pipelines and all module definitions referenced by those pipelines to
     * the specified file.
     *
     * @param pipelines
     * @param destinationPath
     * @throws IOException
     * @throws JAXBException
     */
    public void exportPipelineConfiguration(List<PipelineDefinition> pipelines,
        String destinationPath) throws IOException, JAXBException {
        File destinationFile = new File(destinationPath);
        if (destinationFile.exists() && destinationFile.isDirectory()) {
            throw new IllegalArgumentException(
                "destinationPath exists and is a directory: " + destinationFile);
        }

        log.info("Exporting " + pipelines.size() + " pipelines to: " + destinationFile);

        PipelineDefinitionFile pipelineDefinitionFile = new PipelineDefinitionFile();
        populatePiplineDefinitionFile(pipelines, pipelineDefinitionFile);
        xmlManager.marshal(pipelineDefinitionFile, destinationFile);
    }

    /**
     * Generates the content for the XML document.
     */
    private void populatePiplineDefinitionFile(List<PipelineDefinition> pipelines,
        PipelineDefinitionFile pipelineDefinitionFile) {
        List<String> moduleNames = new LinkedList<>();

        for (PipelineDefinition pipeline : pipelines) {
            pipelineDefinitionFile.getPipelines().add(pipeline);
            for (PipelineDefinitionNode node : pipeline.getNodes()) {
                moduleNames.add(node.getModuleName().getName());
            }
        }
        Collections.sort(moduleNames);
        exportModules(moduleNames, pipelineDefinitionFile);
    }

    /**
     * Stores the module definitions
     *
     * @param moduleList
     */
    private void exportModules(List<String> moduleNames,
        PipelineDefinitionFile pipelineDefinitionFile) {
        PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();

        for (String moduleName : moduleNames) {
            PipelineModuleDefinition module = crud.retrieveLatestVersionForName(moduleName);
            pipelineDefinitionFile.getModules().add(module);
        }
    }

    /**
     * Imports the pipeline, module, and node definitions from a {@link Collection} of files in the
     * appropriate XML format.
     */
    public void importPipelineConfiguration(Collection<File> files) throws Exception {
        for (File sourceFile : files) {
            if (!sourceFile.exists() || sourceFile.isDirectory()) {
                throw new IllegalArgumentException(
                    "sourcePath does not exist or is a directory: " + sourceFile);
            }
        }

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

        log.debug("Importing Module Definitions...");
        importModules(pipelineModuleDefinitions);
        log.debug("Importing Module Definitions...done");

        log.debug("Importing Pipeline Definitions");
        importPipelines(pipelineDefinitions);
        log.debug("DONE importing pipeline configuration");
    }

    private void importModules(List<PipelineModuleDefinition> newModules) {
        PipelineModuleDefinitionCrud moduleCrud = pipelineModuleDefinitionCrud();
        List<PipelineModuleDefinition> existingModules = moduleCrud.retrieveLatestVersions();
        Map<String, PipelineModuleDefinition> existingModuleMap = new HashMap<>();

        for (PipelineModuleDefinition existingModule : existingModules) {
            existingModuleMap.put(existingModule.getName().getName(), existingModule);
        }
        Set<String> existingModuleNames = existingModuleMap.keySet();
        for (PipelineModuleDefinition newModule : newModules) {
            String newModuleName = newModule.getName().getName();
            if (existingModuleNames.contains(newModuleName)) {
                PipelineModuleDefinition databaseModule = null;
                PipelineModuleDefinition existingModule = existingModuleMap.get(newModuleName);
                if (existingModule.isLocked()) {
                    log.info("Adding new version of module: " + newModuleName);
                    databaseModule = existingModule.newVersion();
                } else {
                    databaseModule = existingModule;
                    log.info("Updating unlocked version of module: " + newModuleName);
                }
                databaseModule.setAuditInfo(new AuditInfo());
                databaseModule.setDescription(newModule.getDescription());
                databaseModule.setExeTimeoutSecs(newModule.getExeTimeoutSecs());
                databaseModule.setMinMemoryMegaBytes(newModule.getMinMemoryMegaBytes());

                databaseModule.setPipelineModuleClass(newModule.getPipelineModuleClass());
                databaseModule.setInputsClass(newModule.getInputsClass());
                databaseModule.setOutputsClass(newModule.getOutputsClass());
                newModule = databaseModule;
            } else {
                log.info("Adding new module to module library: " + newModuleName);
            }
            moduleCrud.createOrUpdate(newModule);
        }
    }

    private void importPipelines(List<PipelineDefinition> newPipelineDefinitions) {

        PipelineDefinitionCrud pipelineCrud = pipelineDefinitionCrud();
        List<PipelineDefinition> existingPipelines = pipelineCrud.retrieveLatestVersions();
        Map<String, PipelineDefinition> existingPipelineMap = new HashMap<>();

        for (PipelineDefinition existingPipeline : existingPipelines) {
            existingPipelineMap.put(existingPipeline.getName().getName(), existingPipeline);
        }

        for (PipelineDefinition newPipelineDefinition : newPipelineDefinitions) {

            String pipelineName = newPipelineDefinition.getName().getName();
            PipelineDefinition databasePipeline = null;
            if (existingPipelineMap.containsKey(pipelineName)) {
                PipelineDefinition existingPipeline = existingPipelineMap.get(pipelineName);
                if (existingPipeline.isLocked()) {
                    log.info("Creating new version of pipeline: " + pipelineName);
                } else {
                    log.info("Updating unlocked pipeline: " + pipelineName);
                }
                databasePipeline = existingPipeline
                    .newVersionWithUpdatedContent(newPipelineDefinition);
            } else {
                log.info("Adding new pipeline to pipeline library: " + pipelineName);

                // Note: this is necessary so that we can either manipulate the updated
                // pipeline (use-case above) or the one from XML without triggering a
                // ConcurrentModificationException (which would occur if we set
                // newPipelineDefinition equal to databasePipeline).
                databasePipeline = newPipelineDefinition;
            }
            databasePipeline.setAuditInfo(new AuditInfo());

            Set<PipelineDefinitionNode> nodes = databasePipeline.getNodesFromXml();
            Map<String, PipelineDefinitionNode> nodesByName = new HashMap<>();

            if (nodes.isEmpty()) {
                log.error("Pipeline " + pipelineName + " has no nodes");
            }
            for (PipelineDefinitionNode node : nodes) {
                String childNodeIds = node.getChildNodeNames() != null ? node.getChildNodeNames()
                    : "";
                log.info("Adding node " + node.getModuleName().getName() + " for pipeline "
                    + pipelineName + " with child node(s) " + childNodeIds);
                nodesByName.put(node.getModuleName().getName(), node);
            }

            String rootNodeString = databasePipeline.getRootNodeNames();
            if (rootNodeString != null) {
                log.info("Pipeline " + pipelineName + " root node names: " + rootNodeString);
            } else {
                log.error("Pipeline " + pipelineName + " root node names string is null");
            }

            // pipeline-level parameters
            log.info("Parameter sets for pipeline " + pipelineName + ": "
                + databasePipeline.getParameterSetNames().toString());
            databasePipeline.setPipelineParameterSetNames(
                parseParameterSets(databasePipeline.getParameterSetNames()));

            List<String> rootNodeNames = splitAndListifyString(databasePipeline.getRootNodeNames());
            addNodes(databasePipeline.getName().getName(), rootNodeNames,
                databasePipeline.getRootNodes(), nodesByName);

            pipelineCrud.createOrUpdate(databasePipeline);
        }
    }

    private void addNodes(String pipelineName, List<String> rootNodeNames,
        List<PipelineDefinitionNode> pipelineRootNodes,
        Map<String, PipelineDefinitionNode> xmlNodesByName) {
        PipelineModuleDefinitionCrud moduleCrud = pipelineModuleDefinitionCrud();

        Map<String, DataFileType> dataFileTypeMap = dataFileTypeCrud().retrieveMap();
        Map<String, ModelType> modelTypeMap = modelCrud().retrieveModelTypeMap();

        for (String nodeName : rootNodeNames) {
            PipelineDefinitionNode xmlNode = xmlNodesByName.get(nodeName);
            if (xmlNode == null) {
                throw new PipelineException(
                    "No node found for root node " + nodeName + " in pipeline " + pipelineName);
            }
            PipelineModuleDefinition module = moduleCrud
                .retrieveLatestVersionForName(xmlNode.getModuleName());
            if (module == null) {
                throw new PipelineException("No module found for node " + xmlNode.getModuleName()
                    + " in pipeline " + pipelineName);
            }
            xmlNode.setPipelineName(pipelineName);

            // Module-level parameters
            log.info("Parameter sets for node " + nodeName + ": "
                + xmlNode.getParameterSetNames().toString());
            xmlNode.setModuleParameterSetNames(parseParameterSets(xmlNode.getParameterSetNames()));

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
                throw new PipelineException(xmlNode.getModuleName().getName()
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
                throw new PipelineException(xmlNode.getModuleName().getName()
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
                throw new PipelineException(xmlNode.getModuleName().getName()
                    + " missing model type names: " + missingModelTypes.toString());
            }
            xmlNode.addAllModelTypes(modelTypes);
            pipelineRootNodes.add(xmlNode);

            // Child nodes
            String childNodeIds = xmlNode.getChildNodeNames();

            if (childNodeIds != null && !childNodeIds.isEmpty()) {
                addNodes(pipelineName, splitAndListifyString(childNodeIds), xmlNode.getNextNodes(),
                    xmlNodesByName);
            }
        }
    }

    /**
     * Converts a {@link Set} of {@link ParameterSetName} instances to a {@link Map} that provides
     * the parameter set class as a key and the {@link ParameterSetName} as a value.
     */
    private Map<ClassWrapper<Parameters>, ParameterSetName> parseParameterSets(
        Set<ParameterSetName> parameterSetNames) {
        Map<ClassWrapper<Parameters>, ParameterSetName> parameterSets = new HashMap<>();
        ParameterSetCrud paramCrud = parameterSetCrud();
        for (ParameterSetName pipelineParamSetName : parameterSetNames) {
            String xmlParamName = pipelineParamSetName.getName();
            ParameterSet parameterSet = paramCrud.retrieveLatestVersionForName(xmlParamName);

            if (parameterSet == null) {
                throw new PipelineException("No parameter set found for name: " + xmlParamName);
            }
            parameterSets.put(new ClassWrapper<>(parameterSet), pipelineParamSetName);
        }
        return parameterSets;
    }

    public static List<String> splitAndListifyString(String parentString) {
        return Arrays.asList(parentString.trim().split("\\s*,\\s*"));
    }

    // Methods that supply CRUD instances. These are package private so that they can be
    // replaced by methods that return mocked CRUD instances.
    PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud() {
        if (moduleCrud == null) {
            moduleCrud = new PipelineModuleDefinitionCrud();
        }
        return moduleCrud;
    }

    PipelineDefinitionCrud pipelineDefinitionCrud() {
        if (pipelineCrud == null) {
            pipelineCrud = new PipelineDefinitionCrud();
        }
        return pipelineCrud;
    }

    ParameterSetCrud parameterSetCrud() {
        if (parameterSetCrud == null) {
            parameterSetCrud = new ParameterSetCrud();
        }
        return parameterSetCrud;
    }

    PipelineOperations pipelineOperations() {
        if (pipelineOperations == null) {
            pipelineOperations = new PipelineOperations();
        }
        return pipelineOperations;
    }

    DataFileTypeCrud dataFileTypeCrud() {
        if (dataFileTypeCrud == null) {
            dataFileTypeCrud = new DataFileTypeCrud();
        }
        return dataFileTypeCrud;
    }

    ModelCrud modelCrud() {
        if (modelCrud == null) {
            modelCrud = new ModelCrud();
        }
        return modelCrud;
    }

}
