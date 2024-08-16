package gov.nasa.ziggy.pipeline.definition;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;

/**
 * Exports pipeline definitions to an XML file.
 *
 * @author PT
 */
public class PipelineDefinitionExporter {

    private static final Logger log = LoggerFactory.getLogger(PipelineDefinitionExporter.class);

    private ValidatingXmlManager<PipelineDefinitionFile> xmlManager = new ValidatingXmlManager<>(
        PipelineDefinitionFile.class);
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();

    /**
     * Export the specified pipelines and all module definitions referenced by those pipelines to
     * the specified file.
     *
     * @param pipelines
     * @param destinationPath
     */
    public void exportPipelineConfiguration(List<PipelineDefinition> pipelines,
        String destinationPath) {
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
                moduleNames.add(node.getModuleName());
            }
            pipeline.populateXmlFields();
        }
        Collections.sort(moduleNames);
        for (String moduleName : moduleNames) {
            PipelineModuleDefinition module = pipelineModuleDefinitionOperations()
                .pipelineModuleDefinition(moduleName);
            pipelineDefinitionFile.getModules().add(module);
        }
    }

    PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }
}
