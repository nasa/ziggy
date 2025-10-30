package gov.nasa.ziggy.pipeline.definition.importer;

import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.DATASTORE_NODE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.DATASTORE_REGEXP;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.DATA_FILE_TYPE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.MODEL_TYPE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.PARAMETER_SET;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.PIPELINE;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.PIPELINE_EVENT;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.REMOTE_ENVIRONMENT;
import static gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile.PipelineDefinitionType.STEP;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.data.datastore.DatastoreNode;
import gov.nasa.ziggy.data.datastore.DatastoreRegexp;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineNodeExecutionResources;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.remote.RemoteEnvironment;
import gov.nasa.ziggy.pipeline.xml.ParameterSetDescriptor;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.events.ZiggyEventHandler;

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
    private PipelineImportOperations pipelineImportOperations = new PipelineImportOperations();
    private PipelineOperations pipelineOperations = new PipelineOperations();
    private boolean update;
    private boolean dryRun;

    private List<DatastoreNode> datastoreNodes = new ArrayList<>();
    private List<DatastoreRegexp> datastoreRegexps = new ArrayList<>();
    private List<DataFileType> dataFileTypes = new ArrayList<>();
    private List<ParameterSet> parameterSets = new ArrayList<>();
    private List<ParameterSetDescriptor> parameterSetDescriptors;
    private List<ModelType> modelTypes = new ArrayList<>();
    private List<Pipeline> pipelines = new ArrayList<>();
    private List<PipelineStep> pipelineSteps = new ArrayList<>();
    private List<RemoteEnvironment> remoteEnvironments = new ArrayList<>();
    private List<ZiggyEventHandler> eventHandlers = new ArrayList<>();

    private Collection<Path> files;
    private boolean unmarshalled;

    public PipelineDefinitionImporter(Collection<Path> files) {
        if (CollectionUtils.isEmpty(files)) {
            log.warn("No files present for import");
            return;
        }
        xmlManager = new ValidatingXmlManager<>(PipelineDefinitionFile.class);
        this.files = files;
    }

    /** Unmarshals pipeline definitions from one or more files. */
    private void unmarshalPipelineDefinitions() {
        if (unmarshalled) {
            return;
        }
        for (Path sourceFile : files) {
            if (!Files.exists(sourceFile) || Files.isDirectory(sourceFile)) {
                throw new IllegalArgumentException(
                    "sourcePath does not exist or is a directory: " + sourceFile.toString());
            }
        }

        // Load the content of all the files into appropriate Lists.
        for (Path sourceFile : files) {
            log.info("Unmarshalling pipeline definitions from {}...", sourceFile);
            PipelineDefinitionFile pipelineDefinitionFile = xmlManager.unmarshal(sourceFile);
            pipelines.addAll(pipelineDefinitionFile.getPipelineElements(PIPELINE));
            pipelineSteps.addAll(pipelineDefinitionFile.getPipelineElements(STEP));
            parameterSets.addAll(pipelineDefinitionFile.getPipelineElements(PARAMETER_SET));
            dataFileTypes.addAll(pipelineDefinitionFile.getPipelineElements(DATA_FILE_TYPE));
            modelTypes.addAll(pipelineDefinitionFile.getPipelineElements(MODEL_TYPE));
            datastoreRegexps.addAll(pipelineDefinitionFile.getPipelineElements(DATASTORE_REGEXP));
            datastoreNodes.addAll(pipelineDefinitionFile.getPipelineElements(DATASTORE_NODE));
            eventHandlers.addAll(pipelineDefinitionFile.getPipelineElements(PIPELINE_EVENT));
            remoteEnvironments
                .addAll(pipelineDefinitionFile.getPipelineElements(REMOTE_ENVIRONMENT));
            log.info("Unmarshalling pipeline definitions from {}...done", sourceFile);
        }
        unmarshalled = true;
    }

    /**
     * Returns the {@link ParameterSetDescriptor} instances for all parameter sets across all
     * imported files, plus the parameter sets that are only present in the database.
     */
    public List<ParameterSetDescriptor> parameterSetDescriptors() {
        return new ParameterSetImportConditioner().parameterSetDescriptors(getParameterSets(),
            update);
    }

    public DatastoreImportConditioner datastoreImportConditioner() {
        return new DatastoreImportConditioner(getDataFileTypes(), getModelTypes(),
            getDatastoreRegexps(), getDatastoreNodes());
    }

    public RemoteEnvironmentImportConditioner remoteEnvironmentImportConditioner() {
        return new RemoteEnvironmentImportConditioner(getRemoteEnvironments(), update);
    }

    /**
     * Imports the pipeline, steps, and node definitions from a {@link Collection} of files in the
     * appropriate XML format.
     */
    public void importPipelineDefinitions() {

        // We need all the parameter sets (newly-unmarshaled plus database-only)
        // when importing the pipelines.
        log.debug("Importing parameter sets...");
        parameterSetDescriptors = parameterSetDescriptors();
        log.debug("Importing parameter sets...done");

        log.debug("Importing pipeline steps...");
        new PipelineStepImportConditioner().conditionPipelineSteps(getPipelineSteps(), update);
        log.debug("Importing pipeline steps...done");

        log.debug("Importing datastore definitions...");
        DatastoreImportConditioner datastoreImportConditioner = datastoreImportConditioner();
        datastoreImportConditioner.checkDefinitions();
        log.debug("Importing datastore definitions...done");

        log.debug("Importing pipeline definitions...");
        Map<Pipeline, Set<PipelineNodeExecutionResources>> resourcesByPipeline = new PipelineImportConditioner(
            getPipelineSteps(), parameterSetDescriptors, getDataFileTypes(), getModelTypes(),
            update).importPipelines(getPipelines());
        log.debug("Importing pipeline definitions...done");

        log.debug("Importing event handlers...");
        List<ZiggyEventHandler> validEventHandlers = validEventHandlers();
        log.debug("Importing event handlers...done");

        log.debug("Importing remote environments...");
        List<RemoteEnvironment> remoteEnvironmentsToPersist = remoteEnvironmentImportConditioner()
            .remoteEnvironmentsToPersist();
        log.debug("Importing remote environments...done");

        if (!dryRun) {
            log.debug("Persisting all definitions...");
            pipelineImportOperations().persistClusterDefinition(parameterSetDescriptors,
                datastoreImportConditioner, validEventHandlers, getPipelineSteps(),
                resourcesByPipeline, remoteEnvironmentsToPersist);
            log.debug("Persisting all definitions...done");
        }
    }

    private List<ZiggyEventHandler> validEventHandlers() {
        Set<String> pipelineNames = getPipelines().stream()
            .map(Pipeline::getName)
            .collect(Collectors.toSet());
        pipelineNames.addAll(pipelineOperations().pipelineNames());
        List<ZiggyEventHandler> validEventHandlers = new ArrayList<>();
        for (ZiggyEventHandler handler : getEventHandlers()) {
            if (!pipelineNames.contains(handler.getPipelineName())) {
                log.error("Handler {} fires pipeline {}, which is not defined", handler.getName(),
                    handler.getPipelineName());
                log.error("Not importing handler {}", handler.getName());
                continue;
            }
            validEventHandlers.add(handler);
        }
        return validEventHandlers;
    }

    public static List<ParameterSetDescriptor> parameterSetDescriptors(List<Path> files) {
        PipelineDefinitionImporter pipelineDefinitionImporter = new PipelineDefinitionImporter(
            files);
        pipelineDefinitionImporter.setUpdate(true);
        return pipelineDefinitionImporter.parameterSetDescriptors();
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    private List<Pipeline> getPipelines() {
        unmarshalPipelineDefinitions();
        return pipelines;
    }

    private List<PipelineStep> getPipelineSteps() {
        unmarshalPipelineDefinitions();
        return pipelineSteps;
    }

    public List<ParameterSet> getParameterSets() {
        unmarshalPipelineDefinitions();
        return parameterSets;
    }

    private List<DataFileType> getDataFileTypes() {
        unmarshalPipelineDefinitions();
        return dataFileTypes;
    }

    private List<ModelType> getModelTypes() {
        unmarshalPipelineDefinitions();
        return modelTypes;
    }

    private List<DatastoreRegexp> getDatastoreRegexps() {
        unmarshalPipelineDefinitions();
        return datastoreRegexps;
    }

    private List<DatastoreNode> getDatastoreNodes() {
        unmarshalPipelineDefinitions();
        return datastoreNodes;
    }

    private List<ZiggyEventHandler> getEventHandlers() {
        unmarshalPipelineDefinitions();
        return eventHandlers;
    }

    List<RemoteEnvironment> getRemoteEnvironments() {
        unmarshalPipelineDefinitions();
        return remoteEnvironments;
    }

    PipelineImportOperations pipelineImportOperations() {
        return pipelineImportOperations;
    }

    PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }
}
