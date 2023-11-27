package gov.nasa.ziggy.pipeline;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.models.ModelRegistryOperations;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.parameters.ParametersUtils;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.TypedParameterCollection;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

public class PipelineOperations {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineOperations.class);

    private static final String CSV_REPORT_DELIMITER = ":";

    public PipelineOperations() {
    }

    /**
     * Get the latest version for the specified module name.
     */
    public PipelineModuleDefinition retrieveLatestModuleDefinition(String moduleName) {
        PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
        return crud.retrieveLatestVersionForName(moduleName);
    }

    /**
     * Get the latest version for the specified parameter set name.
     */
    public ParameterSet retrieveLatestParameterSet(String parameterSetName) {
        ParameterSetCrud crud = new ParameterSetCrud();
        return crud.retrieveLatestVersionForName(parameterSetName);
    }

    /**
     * Returns a {@link Set} containing all {@link Parameters} classes required by the specified
     * {@link PipelineDefinitionNode}. This is a union of the Parameters classes required by the
     * PipelineModule itself and the Parameters classes required by the UnitOfWorkTaskGenerator
     * associated with the node.
     */
    public Set<ClassWrapper<ParametersInterface>> retrieveRequiredParameterClassesForNode(
        PipelineDefinitionNode pipelineNode) {
        PipelineModuleDefinitionCrud modDefCrud = new PipelineModuleDefinitionCrud();
        PipelineModuleDefinition modDef = modDefCrud
            .retrieveLatestVersionForName(pipelineNode.getModuleName());

        Set<ClassWrapper<ParametersInterface>> allRequiredParams = new HashSet<>();

        List<Class<? extends ParametersInterface>> uowParams = UnitOfWorkGenerator
            .unitOfWorkGenerator(pipelineNode)
            .newInstance()
            .requiredParameterClasses();
        for (Class<? extends ParametersInterface> uowParam : uowParams) {
            allRequiredParams.add(new ClassWrapper<>(uowParam));
        }
        allRequiredParams.addAll(modDef.getRequiredParameterClasses());

        return allRequiredParams;
    }

    /**
     * Update the specified {@link ParameterSet} with the specified {@link Parameters}.
     * <p>
     * If if the parameters instance is different than the parameter set, then apply the changes. If
     * locked, first create a new version.
     * <p>
     * The new ParameterSet version is returned if one was created, otherwise the old one is
     * returned.
     *
     * @param parameterSetName the name of the parameter set
     * @param parameters the parameters to save
     * @param forceSave if true, save the new ParameterSet even if nothing changed
     * @return the object to use after this call
     */
    public ParameterSet updateParameterSet(String parameterSetName, Parameters parameters,
        boolean forceSave) {
        ParameterSet parameterSet = retrieveLatestParameterSet(parameterSetName);

        return updateParameterSet(parameterSet, parameters, forceSave);
    }

    /**
     * Update the specified {@link ParameterSet} with the specified {@link Parameters}.
     * <p>
     * If if the parameters instance is different than the parameter set, then apply the changes. If
     * locked, first create a new version.
     * <p>
     * The new ParameterSet version is returned if one was created, otherwise the old one is
     * returned.
     */
    public ParameterSet updateParameterSet(ParameterSet parameterSet,
        ParametersInterface newParameters, boolean forceSave) {
        return updateParameterSet(parameterSet, newParameters, parameterSet.getDescription(),
            forceSave);
    }

    /**
     * Update the specified {@link ParameterSet} with the specified {@link Parameters}.
     * <p>
     * If if the parameters instance is different than the parameter set, then apply the changes. If
     * locked, first create a new version.
     *
     * @return the new ParameterSet version if one was created; otherwise the old one is returned
     */
    public ParameterSet updateParameterSet(ParameterSet parameterSet,
        ParametersInterface newParameters, String newDescription, boolean forceSave) {
        ParametersInterface currentParameters = parameterSet.parametersInstance();

        String currentDescription = parameterSet.getDescription();

        boolean descriptionChanged = false;
        if (currentDescription == null) {
            if (newDescription != null) {
                descriptionChanged = true;
            }
        } else if (!currentDescription.equals(newDescription)) {
            descriptionChanged = true;
        }

        if (!compareParameters(currentParameters, newParameters) || descriptionChanged
            || forceSave) {
            parameterSet.setTypedParameters(newParameters.getParameters());
            parameterSet.setDescription(newDescription);
            return new ParameterSetCrud().merge(parameterSet);
        }
        return parameterSet;
    }

    /**
     * Indicates whether the specified parameters contain the same parameters and values.
     *
     * @return true if same or both null
     */
    public boolean compareParameters(ParametersInterface currentParameters,
        ParametersInterface newParameters) {

        if (currentParameters == null) {
            return newParameters == null;
        }
        if (newParameters == null) {
            return false;
        }

        return new TypedParameterCollection(currentParameters.getParameters())
            .totalEquals(new TypedParameterCollection(newParameters.getParameters()));
    }

    /**
     * Create the launcher and launch a new pipeline instance using the specified
     * {@link PipelineDefinition} and startNode/endNode, and with a specified optional name for a
     * {@link ParameterSet} generated by an event handler, if the pipeline launch operation is
     * driven by an event handler.
     */
    public PipelineInstance fireTrigger(PipelineDefinition pipelineDefinition, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode,
        String eventHandlerParamSetName) {
        TriggerValidationResults validationResults = validateTrigger(pipelineDefinition);
        if (validationResults.hasErrors()) {
            throw new PipelineException(
                "Failed to fire trigger, validation failed: " + validationResults.errorReport());
        }

        return pipelineExecutor().launch(pipelineDefinition, instanceName, startNode, endNode,
            eventHandlerParamSetName);
    }

    /**
     * Validates that this {@link PipelineDefinition} is valid for firing. Checks that the
     * associated pipeline definition objects have not changed in an incompatible way and that all
     * {@link ParameterSet}s are set.
     */
    public TriggerValidationResults validateTrigger(PipelineDefinition pipelineDefinition) {
        if (pipelineDefinition == null) {
            throw new NullPointerException("pipelineDefinition must not be null");
        }
        TriggerValidationResults validationResults = new TriggerValidationResults();

        pipelineDefinition.buildPaths();

        validateTriggerParameters(pipelineDefinition, validationResults);

        return validationResults;
    }

    /**
     * Validate that the trigger {@link ParameterSetName}s are all set and match the parameter
     * classes specified in the {@link PipelineDefinition}
     */
    private void validateTriggerParameters(PipelineDefinition pipelineDefinition,
        TriggerValidationResults validationResults) {
        validateParameterClassExists(pipelineDefinition.getPipelineParameterSetNames(),
            "Pipeline parameters", validationResults);

        for (PipelineDefinitionNode rootNode : pipelineDefinition.getRootNodes()) {
            validateTriggerParametersForNode(pipelineDefinition, rootNode, validationResults);
        }
    }

    private void validateTriggerParametersForNode(PipelineDefinition pipelineDefinition,
        PipelineDefinitionNode pipelineDefinitionNode, TriggerValidationResults validationResults) {
        String errorLabel = "module: " + pipelineDefinitionNode.getModuleName();

        Set<ClassWrapper<ParametersInterface>> requiredParameterClasses = retrieveRequiredParameterClassesForNode(
            pipelineDefinitionNode);

        validateParameterClassExists(pipelineDefinitionNode.getModuleParameterSetNames(),
            errorLabel, validationResults);

        validateTriggerParameters(requiredParameterClasses,
            pipelineDefinition.getPipelineParameterSetNames(),
            pipelineDefinitionNode.getModuleParameterSetNames(), errorLabel, validationResults);

        for (PipelineDefinitionNode childNode : pipelineDefinitionNode.getNextNodes()) {
            validateTriggerParametersForNode(pipelineDefinition, childNode, validationResults);
        }
    }

    /**
     * Validate that the trigger {@link ParameterSetName}s are all set and match the parameter
     * classes specified in the {@link PipelineDefinition} for a given trigger node (module)
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void validateTriggerParameters(
        Set<ClassWrapper<ParametersInterface>> requiredModuleParameterClasses,
        Map<ClassWrapper<ParametersInterface>, String> pipelineParameterSetNames,
        Map<ClassWrapper<ParametersInterface>, String> moduleParameterSetNames, String errorLabel,
        TriggerValidationResults validationResults) {
        String paramSetName = null;

        for (ClassWrapper<ParametersInterface> classWrapper : requiredModuleParameterClasses) {
            boolean found = false;

            // check at the module level first
            if (moduleParameterSetNames.containsKey(classWrapper)) {
                paramSetName = moduleParameterSetNames.get(classWrapper);
                found = true;
            } else if (pipelineParameterSetNames.containsKey(classWrapper)) {
                // then at the pipeline level
                paramSetName = pipelineParameterSetNames.get(classWrapper);
                found = true;
            } else {
                validationResults.addError(errorLabel + ": Missing Parameter Set: " + classWrapper
                    + " at either the module level or the pipeline level");
            }

            if (found) {
                if (paramSetName == null) {
                    validationResults.addError(
                        errorLabel + ": trigger parameter Map value for class: " + classWrapper
                            + " is null.  Must be set before firing the TriggerDefinition");
                } else {
                    // check for new fields
                    ParameterSet paramSet = retrieveLatestParameterSet(paramSetName);
                    if (paramSet.hasNewUnsavedFields()) {
                        validationResults.addError(errorLabel + ": parameter set: " + paramSetName
                            + " has new fields that have been added since the last time this parameter set was saved.  "
                            + "Please edit the parameter set in the parameter library, verify that it has the correct values, and save it.");
                    }

                    // Validate the parameters.
                    try {
                        paramSet.parametersInstance().validate();
                    } catch (Exception ex) {
                        validationResults.addError(errorLabel + ": parameter set: " + paramSetName
                            + " has invalid values: " + ex.toString());
                    }
                }
            }
        }

        /*
         * Make sure the same parameters class does not exist at both the pipeline and module level
         * because this makes the data accountability trace less clear (could possibly support this
         * in the future)
         */
        for (ClassWrapper<ParametersInterface> moduleParameterClass : moduleParameterSetNames
            .keySet()) {
            if (pipelineParameterSetNames.containsKey(moduleParameterClass)) {
                validationResults.addError(
                    "Ambiguous configuration: Module parameter and pipeline parameter Maps both contain a value for parameter class: "
                        + moduleParameterClass);
            }
        }
    }

    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    private void validateParameterClassExists(
        Map<ClassWrapper<ParametersInterface>, String> parameterSetNames, String errorLabel,
        TriggerValidationResults validationResults) {
        for (ClassWrapper<ParametersInterface> classWrapper : parameterSetNames.keySet()) {
            try {
                classWrapper.getClazz();
            } catch (RuntimeException e) {
                validationResults.addError(errorLabel
                    + ": trigger parameters contain an entry for a Parameter class that no longer exists: "
                    + classWrapper.getClassName());
            }
        }
    }

    /**
     * Updates the state of a {@link PipelineTask} and simultaneously updates the state of that
     * task's {@link PipelineInstance} if need be. Returns a {@link TaskStateSummary} that has the
     * {@link TaskCounts} for the instance and also the task's {@link PipelineInstanceNode}. This is
     * a safer method to use than the "bare"
     * {@link PipelineTask#setState(gov.nasa.ziggy.pipeline.definition.PipelineTask.State)} because
     * it also automatically updates the pipeline instance, and starts or stops execution clocks as
     * needed.
     */
    public TaskStateSummary setTaskState(long taskId, PipelineTask.State state) {
        return setTaskState(new PipelineTaskCrud().retrieve(taskId), state);
    }

    /**
     * Updates the state of a {@link PipelineTask} and simultaneously updates the state of that
     * task's {@link PipelineInstance} if need be. Returns a {@link TaskStateSummary} that has the
     * {@link TaskCounts} for the instance and also the task's {@link PipelineInstanceNode}. This is
     * a safer method to use than the "bare"
     * {@link PipelineTask#setState(gov.nasa.ziggy.pipeline.definition.PipelineTask.State)} because
     * it also automatically updates the pipeline instance, and starts or stops execution clocks as
     * needed.
     */
    public TaskStateSummary setTaskState(PipelineTask task, PipelineTask.State state) {
        task.setState(state);
        if (state == PipelineTask.State.COMPLETED || state == PipelineTask.State.ERROR
            || state == PipelineTask.State.PARTIAL) {
            task.stopExecutionClock();
        } else {
            task.startExecutionClock();
        }
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
        // First set the task state.
        PipelineTask mergedTask = pipelineTaskCrud.merge(task);

        // Obtain the task counts for the instance.
        TaskCounts instanceCounts = taskCounts(mergedTask.getPipelineInstance());

        // Obtain the task counts for the instance node.
        TaskCounts instanceNodeCounts = taskCounts(mergedTask.getPipelineInstanceNode());

        // Update the instance state
        updateInstanceState(mergedTask, instanceNodeCounts);
        PipelineInstance mergedInstance = new PipelineInstanceCrud()
            .merge(mergedTask.getPipelineInstance());

        return new TaskStateSummary(instanceCounts, instanceNodeCounts, mergedTask, mergedInstance);
    }

    /**
     * Sets the state of a {@link PipelineInstance} based on the {@link TaskCounts} object that
     * summarizes the task states for the instance. Also starts or stops the execution clock as
     * needed. Returns the merged {@link PipelineInstance} object (which includes the updated
     * state).
     */
    public void updateInstanceState(PipelineTask pipelineTask, TaskCounts instanceNodeCounts) {
        PipelineInstance instance = pipelineTask.getPipelineInstance();

        PipelineInstance.State state = PipelineInstance.State.INITIALIZED;

        if (allInstanceNodesComplete(instance)) {

            // If all the instance nodes are done, we can set the instance state to
            // either completed or stalled.
            state = instanceNodeCounts.isInstanceNodeComplete() ? PipelineInstance.State.COMPLETED
                : PipelineInstance.State.ERRORS_STALLED;
        } else if (instanceNodeCounts.isInstanceNodeExecutionComplete()) {

            // If the current node is done, then the state is either stalled or processing.
            state = instanceNodeCounts.isInstanceNodeComplete() ? PipelineInstance.State.PROCESSING
                : PipelineInstance.State.ERRORS_STALLED;
        } else {

            // If the current instance node is still grinding away, then the state is either
            // errors running or processing
            state = instanceNodeCounts.getFailedTaskCount() == 0 ? PipelineInstance.State.PROCESSING
                : PipelineInstance.State.ERRORS_RUNNING;
        }

        state.setExecutionClockState(instance);
        instance.setState(state);
    }

    /**
     * Returns a {@link TaskCounts} instance for a given {@link PipelineInstance}.
     */
    public TaskCounts taskCounts(PipelineInstance pipelineInstance) {
        return TaskCounts.from(new PipelineTaskCrud().retrieveStates(pipelineInstance));
    }

    /**
     * Returns a {@link TaskCounts} instance for a given {@link PipelineInstanceNode}.
     */
    public TaskCounts taskCounts(PipelineInstanceNode pipelineInstanceNode) {
        return TaskCounts.from(new PipelineTaskCrud().retrieveStates(pipelineInstanceNode));
    }

    public TaskCounts taskCounts(long pipelineInstanceNodeId) {
        return TaskCounts.from(new PipelineTaskCrud()
            .retrieveStates(new PipelineInstanceNodeCrud().retrieve(pipelineInstanceNodeId)));
    }

    /**
     * Determines whether all pipeline instances have completed execution. This means that all nodes
     * have tasks that submitted and all tasks for all nodes are either complete or errored.
     */
    public boolean allInstanceNodesComplete(PipelineInstance pipelineInstance) {
        List<PipelineInstanceNode> instanceNodes = new PipelineInstanceNodeCrud()
            .retrieveAll(pipelineInstance);
        for (PipelineInstanceNode node : instanceNodes) {
            if (!taskCounts(node).isInstanceNodeExecutionComplete()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Generate a text report containing pipeline instance metadata including all parameter sets and
     * their values.
     */
    public String generatePedigreeReport(PipelineInstance instance) {
        PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();

        report.append("Instance ID: " + instance.getId() + nl);
        report.append("Instance Name: " + instance.getName() + nl);
        report.append("Instance Priority: " + instance.getPriority() + nl);
        report.append("Instance State: " + instance.getState() + nl);
        List<String> instanceSoftwareRevisions = pipelineTaskCrud
            .distinctSoftwareRevisions(instance);
        report.append("Instance Software Revisions: " + instanceSoftwareRevisions + nl);
        report.append(nl);
        report.append("Definition Name: " + instance.getPipelineDefinition().getName() + nl);
        report.append("Definition Version: " + instance.getPipelineDefinition().getVersion() + nl);

        report.append(nl);
        report.append("Pipeline Parameter Sets" + nl);
        Map<ClassWrapper<ParametersInterface>, ParameterSet> pipelineParamSets = instance
            .getPipelineParameterSets();
        for (ClassWrapper<ParametersInterface> paramClassWrapper : pipelineParamSets.keySet()) {
            ParameterSet paramSet = pipelineParamSets.get(paramClassWrapper);

            appendParameterSetToReport(report, paramSet, "  ", false);
            report.append(nl);
        }

        report.append(nl);
        report.append("Modules" + nl);

        List<PipelineInstanceNode> pipelineNodes = pipelineInstanceNodeCrud.retrieveAll(instance);

        for (PipelineInstanceNode node : pipelineNodes) {
            PipelineModuleDefinition module = node.getPipelineModuleDefinition();
            TaskCounts instanceNodeCounts = taskCounts(node);

            appendModule(nl, report, module);

            report
                .append("    # Tasks (total/completed/failed): " + instanceNodeCounts.getTaskCount()
                    + "/" + instanceNodeCounts.getCompletedTaskCount() + "/"
                    + instanceNodeCounts.getFailedTaskCount() + nl);
            List<String> nodeSoftwareRevisions = pipelineTaskCrud.distinctSoftwareRevisions(node);
            report.append("    Software Revisions for node:" + nodeSoftwareRevisions + nl);

            Map<ClassWrapper<ParametersInterface>, ParameterSet> moduleParamSets = node
                .getModuleParameterSets();
            for (ClassWrapper<ParametersInterface> paramClassWrapper : moduleParamSets.keySet()) {
                ParameterSet moduleParamSet = moduleParamSets.get(paramClassWrapper);

                appendParameterSetToReport(report, moduleParamSet, "    ", false);
                report.append(nl);
            }
        }

        report.append(nl);
        report.append("Data Model Registry" + nl);
        ModelRegistryOperations modelMetadataOps = new ModelRegistryOperations();
        report.append(modelMetadataOps.report(instance));

        return report.toString();
    }

    /**
     * Generate a text report about the specified {@link PipelineDefinition} including all parameter
     * sets and their values.
     */
    public String generatePipelineReport(PipelineDefinition pipelineDefinition) {
        String nl = System.lineSeparator();
        StringBuilder report = new StringBuilder();
        ParameterSetCrud paramSetCrud = new ParameterSetCrud();
        PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();

        report.append("Pipeline version: " + pipelineDefinition.getVersion() + nl);
        report.append("Pipeline name: " + pipelineDefinition.getName() + nl);
        report.append("Pipeline priority: " + pipelineDefinition.getInstancePriority() + nl);
        report.append(nl);

        TriggerValidationResults validationErrors = validateTrigger(pipelineDefinition);
        if (validationErrors.hasErrors()) {
            report.append("*** Pipeline Validation Errors ***" + nl);
            report.append(nl);
            report.append(validationErrors.errorReport("  "));
            report.append(nl);
        }

        report.append("Pipeline Parameter Sets" + nl);
        Map<ClassWrapper<ParametersInterface>, String> pipelineParamSets = pipelineDefinition
            .getPipelineParameterSetNames();
        for (ClassWrapper<ParametersInterface> paramClassWrapper : pipelineParamSets.keySet()) {
            String paramSetName = pipelineParamSets.get(paramClassWrapper);
            ParameterSet paramSet = paramSetCrud.retrieveLatestVersionForName(paramSetName);

            report.append(nl);
            appendParameterSetToReport(report, paramSet, "  ", false);
        }

        report.append(nl);
        report.append("Modules" + nl);

        List<PipelineDefinitionNode> nodes = pipelineDefinition.getNodes();
        for (PipelineDefinitionNode node : nodes) {
            String moduleName = node.getModuleName();

            PipelineModuleDefinition modDef = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(moduleName);

            appendModule(nl, report, modDef);

            Map<ClassWrapper<ParametersInterface>, String> moduleParamSetNames = node
                .getModuleParameterSetNames();
            for (ClassWrapper<ParametersInterface> paramClassWrapper : moduleParamSetNames
                .keySet()) {
                String moduleParamSetName = moduleParamSetNames.get(paramClassWrapper);
                ParameterSet moduleParamSet = paramSetCrud
                    .retrieveLatestVersionForName(moduleParamSetName);

                appendParameterSetToReport(report, moduleParamSet, "    ", false);
            }
        }

        return report.toString();
    }

    private void appendModule(String nl, StringBuilder report, PipelineModuleDefinition module) {
        report.append(nl);
        report.append(
            "  Module definition: " + module.getName() + ", version=" + module.getVersion() + nl);
        report.append("    Java classname: "
            + module.getPipelineModuleClass().getClazz().getSimpleName() + nl);
        report.append("    exe timeout seconds: " + module.getExeTimeoutSecs() + nl);
        report.append("    min memory MB: " + module.getMinMemoryMegaBytes() + nl);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void exportPipelineParams(PipelineDefinition pipelineDefinition,
        File destinationDirectory) {
        if (!destinationDirectory.exists()) {
            try {
                FileUtils.forceMkdir(destinationDirectory);
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Unable to create directory " + destinationDirectory.toString(), e);
            }
        }

        ParameterSetCrud paramSetCrud = new ParameterSetCrud();
        Map<String, Parameters> paramsToExport = new HashMap<>();

        Map<ClassWrapper<ParametersInterface>, String> pipelineParamSets = pipelineDefinition
            .getPipelineParameterSetNames();
        for (ClassWrapper<ParametersInterface> paramClassWrapper : pipelineParamSets.keySet()) {
            String paramSetName = pipelineParamSets.get(paramClassWrapper);
            ParameterSet paramSet = paramSetCrud.retrieveLatestVersionForName(paramSetName);

            paramsToExport.put(paramSetName, paramSet.parametersInstance());
        }

        List<PipelineDefinitionNode> nodes = pipelineDefinition.getNodes();
        for (PipelineDefinitionNode node : nodes) {
            Map<ClassWrapper<ParametersInterface>, String> moduleParamSetNames = node
                .getModuleParameterSetNames();
            for (ClassWrapper<ParametersInterface> paramClassWrapper : moduleParamSetNames
                .keySet()) {
                String moduleParamSetName = moduleParamSetNames.get(paramClassWrapper);
                ParameterSet moduleParamSet = paramSetCrud
                    .retrieveLatestVersionForName(moduleParamSetName);

                paramsToExport.put(moduleParamSetName, moduleParamSet.parametersInstance());
            }
        }

        for (String paramSetName : paramsToExport.keySet()) {
            Parameters params = paramsToExport.get(paramSetName);
            File file = new File(destinationDirectory, paramSetName + ".properties");

            ParametersUtils.exportParameters(file, params);
        }
    }

    /**
     * Creates a textual report of all ParameterSets in the Parameter Library, including name, type,
     * keys and values.
     */
    public String generateParameterLibraryReport(boolean csvMode) {
        StringBuilder report = new StringBuilder();

        ParameterSetCrud paramSetCrud = new ParameterSetCrud();
        List<ParameterSet> allParamSets = paramSetCrud.retrieveLatestVersions();

        for (ParameterSet parameterSet : allParamSets) {
            appendParameterSetToReport(report, parameterSet, "", csvMode);
        }

        return report.toString();
    }

    /**
     * Used by generatePedigreeReport
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public void appendParameterSetToReport(StringBuilder report, ParameterSet paramSet,
        String indent, boolean csvMode) {
        String nl = System.lineSeparator();
        String paramsIndent = indent + "  ";
        String parameterClassName = "";

        try {
            parameterClassName = Class.forName(paramSet.getClassname()).getSimpleName();
        } catch (RuntimeException | ClassNotFoundException e) {
            parameterClassName = " <deleted>: " + paramSet.getClassname();
        }

        if (!csvMode) {
            report.append(indent + "Parameter set: " + paramSet.getName() + " (type="
                + parameterClassName + ", version=" + paramSet.getVersion() + ")" + nl);
        }

        Set<TypedParameter> parameters = paramSet.parametersInstance().getParameters();
        if (parameters.isEmpty() && !csvMode) {
            report.append(paramsIndent + "(no parameters)" + nl);
        } else {
            for (TypedParameter parameter : parameters) {
                String key = parameter.getName();
                String value = parameter.getString();

                if (csvMode) {
                    report.append(paramSet.getName() + CSV_REPORT_DELIMITER);
                    report.append(parameterClassName + CSV_REPORT_DELIMITER);
                    report.append(paramSet.getVersion() + CSV_REPORT_DELIMITER);
                    report.append(key + CSV_REPORT_DELIMITER);
                    report.append(value + nl);
                } else {
                    report.append(paramsIndent + key + " = " + value + nl);
                }
            }
        }
    }

    public Map<ClassWrapper<ParametersInterface>, ParameterSet> retrieveParameterSets(
        PipelineDefinition pipelineDefinition, String moduleName) {
        Map<ClassWrapper<ParametersInterface>, String> parameterSetNameMap = new HashMap<>(
            pipelineDefinition.getPipelineParameterSetNames());
        for (PipelineDefinitionNode pipelineDefinitionNode : pipelineDefinition.getNodes()) {
            if (pipelineDefinitionNode.getModuleName().equals(moduleName)) {
                parameterSetNameMap.putAll(pipelineDefinitionNode.getModuleParameterSetNames());
            }
        }

        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSetMap = new HashMap<>();
        for (Entry<ClassWrapper<ParametersInterface>, String> entry : parameterSetNameMap
            .entrySet()) {
            ParameterSet parameterSet = parameterSetCrud
                .retrieveLatestVersionForName(entry.getValue());
            parameterSetMap.put(entry.getKey(), parameterSet);
        }

        return parameterSetMap;
    }

    public PipelineExecutor pipelineExecutor() {
        return new PipelineExecutor();
    }

    /**
     * Container used to provide information to the caller after execution of the
     * {@link PipelineOperations#setTaskState(PipelineTask, gov.nasa.ziggy.pipeline.definition.PipelineTask.State)}.
     * The updated {@link PipelineTask} and {@link PipelineInstance} are returned, as are the
     * {@link TaskCounts} instances for the pipeline instance and instance node for the task.
     *
     * @author PT
     */
    public static class TaskStateSummary {

        private final TaskCounts instanceCounts;
        private final TaskCounts instanceNodeCounts;
        private final PipelineTask task;
        private final PipelineInstance instance;

        public TaskStateSummary(TaskCounts instanceCounts, TaskCounts instanceNodeCounts,
            PipelineTask mergedTask, PipelineInstance mergedInstance) {
            this.instanceCounts = instanceCounts;
            this.instanceNodeCounts = instanceNodeCounts;
            task = mergedTask;
            instance = mergedInstance;
        }

        public TaskCounts getInstanceCounts() {
            return instanceCounts;
        }

        public TaskCounts getInstanceNodeCounts() {
            return instanceNodeCounts;
        }

        public PipelineTask getTask() {
            return task;
        }

        public PipelineInstance getInstance() {
            return instance;
        }
    }
}
