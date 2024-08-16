package gov.nasa.ziggy.pipeline;

import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.models.ModelOperations;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
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
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();

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
        List<String> instanceSoftwareRevisions = pipelineInstanceOperations()
            .distinctSoftwareRevisions(instance);
        report.append("Instance Software Revisions: " + instanceSoftwareRevisions + nl);
        report.append(nl);
        report.append("Definition Name: " + instance.getPipelineDefinition().getName() + nl);
        report.append("Definition Version: " + instance.getPipelineDefinition().getVersion() + nl);

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
            PipelineModuleDefinition module = node.getPipelineModuleDefinition();
            TaskCounts instanceNodeCounts = pipelineInstanceNodeOperations().taskCounts(node);

            appendModule(nl, report, module);

            report.append(FOUR_SPACE_INDENT + "# Tasks (total/completed/failed): "
                + instanceNodeCounts.getTaskCount() + "/"
                + instanceNodeCounts.getTotalCounts().getCompletedTaskCount() + "/"
                + instanceNodeCounts.getTotalCounts().getFailedTaskCount() + nl);
            List<String> nodeSoftwareRevisions = pipelineInstanceNodeOperations()
                .distinctSoftwareRevisions(node);
            report.append(
                FOUR_SPACE_INDENT + "Software Revisions for node:" + nodeSoftwareRevisions + nl);

            Set<ParameterSet> moduleParameterSets = pipelineInstanceNodeOperations()
                .parameterSets(node);
            for (ParameterSet moduleParamSet : moduleParameterSets) {
                appendParameterSetToReport(report, moduleParamSet, FOUR_SPACE_INDENT, false);
                report.append(nl);
            }
        }

        report.append(nl);
        report.append("Data Model Registry" + nl);
        ModelOperations modelMetadataOps = new ModelOperations();
        report.append(modelMetadataOps.report(instance));

        return report.toString();
    }

    private void appendModule(String nl, StringBuilder report, PipelineModuleDefinition module) {
        report.append(nl);
        report.append(TWO_SPACE_INDENT + "Module definition: " + module.getName() + ", version="
            + module.getVersion() + nl);
        report.append(FOUR_SPACE_INDENT + "Java classname: "
            + module.getPipelineModuleClass().getClazz().getSimpleName() + nl);
    }

    /**
     * Generate a text report about the specified {@link PipelineDefinition} including all parameter
     * sets and their values.
     */
    public String generatePipelineReport(PipelineDefinition pipelineDefinition) {
        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        report.append("Pipeline version: " + pipelineDefinition.getVersion() + nl);
        report.append("Pipeline name: " + pipelineDefinition.getName() + nl);
        report.append("Pipeline priority: " + pipelineDefinition.getInstancePriority() + nl);
        report.append(nl);

        report.append("Pipeline Parameter Sets" + nl);
        Set<String> pipelineParameterSetNames = pipelineDefinitionOperations()
            .parameterSetNames(pipelineDefinition);
        for (String paramSetName : pipelineParameterSetNames) {
            ParameterSet paramSet = parametersOperations().parameterSet(paramSetName);

            report.append(nl);
            appendParameterSetToReport(report, paramSet, TWO_SPACE_INDENT, false);
        }

        report.append(nl);
        report.append("Nodes" + nl);

        List<PipelineDefinitionNode> nodes = pipelineDefinitionNodeOperations()
            .pipelineDefinitionNodesForPipelineDefinition(pipelineDefinition.getName());
        pipelineDefinition.getNodes();
        for (PipelineDefinitionNode node : nodes) {
            String moduleName = node.getModuleName();

            PipelineModuleDefinition modDef = pipelineModuleDefinitionOperations()
                .pipelineModuleDefinition(moduleName);

            appendModule(nl, report, modDef);

            Set<String> parameterSetNames = pipelineDefinitionNodeOperations()
                .parameterSetNames(node);
            for (String moduleParamSetName : parameterSetNames) {
                ParameterSet moduleParamSet = parametersOperations()
                    .parameterSet(moduleParamSetName);

                appendParameterSetToReport(report, moduleParamSet, FOUR_SPACE_INDENT, false);
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

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }

    PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
    }

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }
}
