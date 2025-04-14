package gov.nasa.ziggy.pipeline.definition;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.definition.importer.PipelineDefinitionFile;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
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
    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();

    public void exportPipelineConfiguration(List<Pipeline> pipelines,
        List<ParameterSet> parameterSets, String destinationPath) {
        File destinationFile = new File(destinationPath);
        if (destinationFile.exists() && destinationFile.isDirectory()) {
            throw new IllegalArgumentException(
                "destinationPath exists and is a directory: " + destinationFile);
        }

        PipelineDefinitionFile pipelineDefinitionFile = new PipelineDefinitionFile();
        populatePipelineDefinitionFile(pipelines, parameterSets, pipelineDefinitionFile);
        log.info("Exporting to file {}", destinationFile.getAbsolutePath());
        xmlManager.marshal(pipelineDefinitionFile, destinationFile);
    }

    /**
     * Generates the content for the XML document. Only the {@link Pipeline} and
     * {@link ParameterSet} instances are needed as arguments because the rest of the pipeline
     * definition content can be obtained from the contents of those elements.
     */
    private void populatePipelineDefinitionFile(List<Pipeline> pipelines,
        List<ParameterSet> parameterSets, PipelineDefinitionFile pipelineDefinitionFile) {
        List<String> pipelineStepNames = new LinkedList<>();

        if (!CollectionUtils.isEmpty(pipelines)) {
            log.info("Adding {} pipelines to export", pipelines.size());

            for (Pipeline pipeline : pipelines) {
                pipelineDefinitionFile.getPipelineElements().add(pipeline);
                for (PipelineNode node : pipeline.getNodes()) {
                    pipelineStepNames.add(node.getPipelineStepName());
                }
                pipeline.populateXmlFields();
            }
            Collections.sort(pipelineStepNames);
            for (String pipelineStepName : pipelineStepNames) {
                PipelineStep pipelineStep = pipelineStepOperations().pipelineStep(pipelineStepName);
                pipelineDefinitionFile.getPipelineElements().add(pipelineStep);
            }
        }
        if (!CollectionUtils.isEmpty(parameterSets)) {
            log.info("Adding {} parameter sets to export", parameterSets.size());
            for (ParameterSet parameterSet : parameterSets) {
                parameterSet.populateXmlFields();
                pipelineDefinitionFile.getPipelineElements().add(parameterSet);
            }
        }
    }

    PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }
}
