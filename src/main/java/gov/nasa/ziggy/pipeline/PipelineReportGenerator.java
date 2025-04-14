package gov.nasa.ziggy.pipeline;

import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Generates reports on pipelines, parameter sets, etc.
 *
 * @author PT
 */
public class PipelineReportGenerator {

    private static final String NO_INDENT = "";
    private static final String TWO_SPACE_INDENT = "  ";
    private static final String FOUR_SPACE_INDENT = "    ";

    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();
    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private PipelineOperations pipelineOperations = new PipelineOperations();

    /**
     * Creates a textual report of all ParameterSets in the Parameter Library, including name, type,
     * keys and values.
     */
    public String generateParameterLibraryReport(boolean csvMode) {
        StringBuilder report = new StringBuilder();
        List<ParameterSet> allParamSets = parametersOperations().parameterSets();
        for (ParameterSet parameterSet : allParamSets) {
            appendParameterSetToReport(report, parameterSet, NO_INDENT, csvMode);
        }
        return report.toString();
    }

    /**
     * Used by generatePedigreeReport
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void appendParameterSetToReport(StringBuilder report, ParameterSet paramSet,
        String indent, boolean csvMode) {
        String nl = System.lineSeparator();
        String paramsIndent = indent + "  ";

        if (!csvMode) {
            report.append(indent + "Parameter set: " + paramSet.getName() + " (version="
                + paramSet.getVersion() + ")" + nl);
        }

        Set<Parameter> parameters = paramSet.getParameters();
        if (parameters.isEmpty() && !csvMode) {
            report.append(paramsIndent + "(no parameters)" + nl);
        } else {
            for (Parameter parameter : parameters) {
                String key = parameter.getName();
                String value = parameter.getString();

                if (csvMode) {
                    report.append(paramSet.getName() + PipelineTaskOperations.CSV_REPORT_DELIMITER);
                    report.append(
                        paramSet.getVersion() + PipelineTaskOperations.CSV_REPORT_DELIMITER);
                    report.append(key + PipelineTaskOperations.CSV_REPORT_DELIMITER);
                    report.append(value + nl);
                } else {
                    report.append(paramsIndent + key + " = " + value + nl);
                }
            }
        }
    }

    /**
     * Generate a text report containing pipeline instance metadata including all parameter sets and
     * their values.
     */
    public String generatePedigreeReport(PipelineInstance instance) {

        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        report.append("Instance ID: " + instance.getId() + nl);
        report.append("Instance Name: " + instance.getName() + nl);
        report.append("Instance Priority: " + instance.getPriority() + nl);
        report.append("Instance State: " + instance.getState() + nl);
        List<String> instanceSoftwareRevisions = pipelineTaskDataOperations()
            .distinctSoftwareRevisions(instance);
        report.append("Instance Software Revisions: " + instanceSoftwareRevisions + nl);
        report.append(nl);
        report.append("Pipeline Name: " + instance.getPipeline().getName() + nl);
        report.append("Pipeline Version: " + instance.getPipeline().getVersion() + nl);

        report.append(nl);
        report.append("Pipeline Parameter Sets" + nl);
        Set<ParameterSet> pipelineParamSets = pipelineInstanceOperations().parameterSets(instance);
        for (ParameterSet paramSet : pipelineParamSets) {

            appendParameterSetToReport(report, paramSet, TWO_SPACE_INDENT, false);
            report.append(nl);
        }

        report.append(nl);
        report.append("Nodes" + nl);

        List<PipelineInstanceNode> pipelineNodes = pipelineInstanceOperations()
            .instanceNodes(instance);

        for (PipelineInstanceNode node : pipelineNodes) {
            PipelineStep pipelineStep = node.getPipelineStep();
            TaskCounts instanceNodeCounts = pipelineTaskDisplayDataOperations().taskCounts(node);

            appendPipelineStep(nl, report, pipelineStep);

            report.append(FOUR_SPACE_INDENT + "# Tasks (total/completed/failed): "
                + instanceNodeCounts.getTaskCount() + "/"
                + instanceNodeCounts.getTotalCounts().getCompletedTaskCount() + "/"
                + instanceNodeCounts.getTotalCounts().getFailedTaskCount() + nl);
            List<String> nodeSoftwareRevisions = pipelineTaskDataOperations()
                .distinctSoftwareRevisions(node);
            report.append(
                FOUR_SPACE_INDENT + "Software Revisions for node:" + nodeSoftwareRevisions + nl);

            Set<ParameterSet> parameterSets = pipelineInstanceNodeOperations().parameterSets(node);
            for (ParameterSet parameterSet : parameterSets) {
                appendParameterSetToReport(report, parameterSet, FOUR_SPACE_INDENT, false);
                report.append(nl);
            }
        }

        report.append(nl);
        report.append("Data Model Registry" + nl);
        ModelOperations modelMetadataOps = new ModelOperations();
        report.append(modelMetadataOps.report(instance));

        return report.toString();
    }

    private void appendPipelineStep(String nl, StringBuilder report, PipelineStep pipelineStep) {
        report.append(nl);
        report.append(TWO_SPACE_INDENT + "Pipeline step: " + pipelineStep.getName() + ", version="
            + pipelineStep.getVersion() + nl);
        report.append(FOUR_SPACE_INDENT + "Java classname: "
            + pipelineStep.getPipelineStepExecutorClass().getClazz().getSimpleName() + nl);
    }

    /**
     * Generate a text report about the specified {@link Pipeline} including all parameter sets and
     * their values.
     */
    public String generatePipelineReport(Pipeline pipeline) {
        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        report.append("Pipeline version: " + pipeline.getVersion() + nl);
        report.append("Pipeline name: " + pipeline.getName() + nl);
        report.append("Pipeline priority: " + pipeline.getInstancePriority() + nl);
        report.append(nl);

        report.append("Pipeline Parameter Sets" + nl);
        Set<String> pipelineParameterSetNames = pipelineOperations().parameterSetNames(pipeline);
        for (String paramSetName : pipelineParameterSetNames) {
            ParameterSet paramSet = parametersOperations().parameterSet(paramSetName);

            report.append(nl);
            appendParameterSetToReport(report, paramSet, TWO_SPACE_INDENT, false);
        }

        report.append(nl);
        report.append("Nodes" + nl);

        List<PipelineNode> nodes = pipelineNodeOperations()
            .pipelineNodesForPipeline(pipeline.getName());
        pipeline.getNodes();
        for (PipelineNode node : nodes) {
            String pipelineStepName = node.getPipelineStepName();

            PipelineStep pipelineStep = pipelineStepOperations().pipelineStep(pipelineStepName);

            appendPipelineStep(nl, report, pipelineStep);

            Set<String> parameterSetNames = pipelineNodeOperations().parameterSetNames(node);
            for (String parameterSetName : parameterSetNames) {
                ParameterSet parameterSet = parametersOperations().parameterSet(parameterSetName);
                appendParameterSetToReport(report, parameterSet, FOUR_SPACE_INDENT, false);
            }
        }

        return report.toString();
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }

    PipelineNodeOperations pipelineNodeOperations() {
        return pipelineNodeOperations;
    }

    PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }
}
