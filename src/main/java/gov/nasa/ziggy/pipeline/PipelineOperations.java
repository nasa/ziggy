package gov.nasa.ziggy.pipeline;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import gov.nasa.ziggy.parameters.ParametersUtils;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ModuleName;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSetName;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.messages.WorkerFireTriggerRequest;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

public class PipelineOperations {
    private static final Logger log = LoggerFactory.getLogger(PipelineOperations.class);

    private static final String CSV_REPORT_DELIMITER = ":";

    public PipelineOperations() {
    }

    /**
     * Get the latest version for the specified {@link ModuleName}
     *
     * @param moduleName
     * @return
     */
    public PipelineModuleDefinition retrieveLatestModuleDefinition(ModuleName moduleName) {
        PipelineModuleDefinitionCrud crud = new PipelineModuleDefinitionCrud();
        PipelineModuleDefinition latestModuleDef = crud.retrieveLatestVersionForName(moduleName);

        return latestModuleDef;
    }

    /**
     * Get the latest version for the specified parameterSetName
     *
     * @param parameterSetName
     * @return
     */
    public ParameterSet retrieveLatestParameterSet(String parameterSetName) {
        ParameterSetCrud crud = new ParameterSetCrud();
        ParameterSet latestParameterSet = crud.retrieveLatestVersionForName(parameterSetName);

        return latestParameterSet;
    }

    /**
     * Get the latest version for the specified {@link ParameterSetName}
     *
     * @param parameterSetName
     * @return
     */
    public ParameterSet retrieveLatestParameterSet(ParameterSetName parameterSetName) {
        ParameterSetCrud crud = new ParameterSetCrud();
        ParameterSet latestParameterSet = crud.retrieveLatestVersionForName(parameterSetName);

        return latestParameterSet;
    }

    /**
     * Returns a {@link Set} containing all {@link Parameters} classes required by the specified
     * {@link PipelineDefinitionNode}. This is a union of the Parameters classes required by the
     * PipelineModule itself and the Parameters classes required by the UnitOfWorkTaskGenerator
     * associated with the node.
     *
     * @param pipelineNode
     * @return
     */
    public Set<ClassWrapper<Parameters>> retrieveRequiredParameterClassesForNode(
        PipelineDefinitionNode pipelineNode) {
        PipelineModuleDefinitionCrud modDefCrud = new PipelineModuleDefinitionCrud();
        PipelineModuleDefinition modDef = modDefCrud
            .retrieveLatestVersionForName(pipelineNode.getModuleName());

        Set<ClassWrapper<Parameters>> allRequiredParams = new HashSet<>();

        List<Class<? extends Parameters>> uowParams = UnitOfWorkGenerator
            .unitOfWorkGenerator(pipelineNode)
            .newInstance()
            .requiredParameterClasses();
        for (Class<? extends Parameters> uowParam : uowParams) {
            allRequiredParams.add(new ClassWrapper<Parameters>(uowParam));
        }
        allRequiredParams.addAll(modDef.getRequiredParameterClasses());

        return allRequiredParams;
    }

    /**
     * Update the specified {@link ParameterSetName} with the specified {@link Parameters}.
     * <p>
     * If if the parameters instance is different than the parameter set, then apply the changes. If
     * locked, first create a new version.
     * <p>
     * The new ParameterSet version is returned if one was created, otherwise the old one is
     * returned.
     *
     * @param parameters
     * @param forceSave If true, save the new ParameterSet even if nothing changed
     * @return
     */
    public ParameterSet updateParameterSet(ParameterSetName parameterSetName, Parameters parameters,
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
     *
     * @param parameterSet
     * @return
     */
    public ParameterSet updateParameterSet(ParameterSet parameterSet, Parameters newParameters,
        boolean forceSave) {
        return updateParameterSet(parameterSet, newParameters, parameterSet.getDescription(),
            forceSave);
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
     * @param newParameters
     * @return
     */
    public ParameterSet updateParameterSet(ParameterSet parameterSet, Parameters newParameters,
        String newDescription, boolean forceSave) {
        BeanWrapper<Parameters> currentParamsBean = parameterSet.getParameters();
        BeanWrapper<Parameters> newParamsBean = new BeanWrapper<>(newParameters);

        String currentDescription = parameterSet.getDescription();

        ParameterSet updatedParameterSet = parameterSet;

        boolean descriptionChanged = false;
        if (currentDescription == null) {
            if (newDescription != null) {
                descriptionChanged = true;
            }
        } else if (!currentDescription.equals(newDescription)) {
            descriptionChanged = true;
        }

        boolean propsChanged = !compareParameters(currentParamsBean, newParamsBean);

        if (propsChanged || descriptionChanged || forceSave) {
            ParameterSetCrud crud = new ParameterSetCrud();
            if (parameterSet.isLocked()) {
                updatedParameterSet = parameterSet.newVersion();
                // Evict the exiting parameter set object so that we don't get
                // a Hibernate error about a duplicate ParameterSetName.
                crud.evict(parameterSet);
            }

            updatedParameterSet.setParameters(newParamsBean);
            updatedParameterSet.setDescription(newDescription);
            crud.create(updatedParameterSet);
        }

        return updatedParameterSet;
    }

    /**
     * Indicates whether the specified beans contain the same parameters and values
     *
     * @param currentParamsBean
     * @param newParamsBean
     * @return true if same
     */
    public boolean compareParameters(BeanWrapper<Parameters> currentParamsBean,
        BeanWrapper<Parameters> newParamsBean) {
        Map<String, String> currentProps = BeanWrapper
            .propertyValueByName(currentParamsBean.getTypedProperties());
        Map<String, String> newProps = BeanWrapper
            .propertyValueByName(newParamsBean.getTypedProperties());

        boolean propsSame = true;
        if (currentProps == null) {
            if (newProps != null) {
                propsSame = false;
            }
        } else {
            propsSame = currentProps.equals(newProps);
        }

        log.debug("currentProps.size = " + currentProps.size());
        log.debug("newProps.size = " + newProps.size());

        return propsSame;
    }

    /**
     * Sends a fire-trigger request message to the worker.
     */
    public void sendTriggerMessage(WorkerFireTriggerRequest triggerRequest) {
        new PipelineExecutor().sendTriggerMessage(triggerRequest);
    }

    /**
     * Sends a request for information on whether any pipelines are running or queued.
     */
    public void sendRunningPipelinesCheckRequestMessage() {
        new PipelineExecutor().sendRunningPipelinesCheckRequestMessage();
    }

    /**
     * Create the launcher and launch a new pipeline instance using the specified
     * {@link PipelineDefinition} and startNode/endNode
     */
    public InstanceAndTasks fireTrigger(PipelineDefinition pipelineDefinition, String instanceName,
        PipelineDefinitionNode startNode, PipelineDefinitionNode endNode) {
        return fireTrigger(pipelineDefinition, instanceName, startNode, endNode, null);
    }

    /**
     * Create the launcher and launch a new pipeline instance using the specified
     * {@link PipelineDefinition} and startNode/endNode, and with a specified optional name for a
     * {@link ParameterSet} generated by an event handler, if the pipeline launch operation is
     * driven by an event handler.
     */
    public InstanceAndTasks fireTrigger(PipelineDefinition pipelineDefinition, String instanceName,
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

    public void sendWorkerMessageForTask(PipelineTask task) {
        pipelineExecutor().sendWorkerMessageForTask(task);
    }

    /**
     * Validates that this {@link PipelineDefinition} is valid for firing. Checks that the
     * associated pipeline definition objects have not changed in an incompatible way and that all
     * {@link ParameterSetName}s are set.
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
     *
     * @param pipelineDefinition
     * @param validationResults
     */
    private void validateTriggerParameters(PipelineDefinition pipelineDefinition,
        TriggerValidationResults validationResults) {
        validateParameterClassExists(pipelineDefinition.getPipelineParameterSetNames(),
            "Pipeline parameters", validationResults);

        for (PipelineDefinitionNode rootNode : pipelineDefinition.getRootNodes()) {
            validateTriggerParametersForNode(pipelineDefinition, rootNode, validationResults);
        }
    }

    /**
     * @param pipelineDefinition
     * @param validationResults
     */
    private void validateTriggerParametersForNode(PipelineDefinition pipelineDefinition,
        PipelineDefinitionNode pipelineDefinitionNode, TriggerValidationResults validationResults) {
        String errorLabel = "module: " + pipelineDefinitionNode.getModuleName();

        Set<ClassWrapper<Parameters>> requiredParameterClasses = retrieveRequiredParameterClassesForNode(
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
     *
     * @param validationResults
     */
    private void validateTriggerParameters(
        Set<ClassWrapper<Parameters>> requiredModuleParameterClasses,
        Map<ClassWrapper<Parameters>, ParameterSetName> pipelineParameterSetNames,
        Map<ClassWrapper<Parameters>, ParameterSetName> moduleParameterSetNames, String errorLabel,
        TriggerValidationResults validationResults) {
        ParameterSetName paramSetName = null;

        for (ClassWrapper<Parameters> classWrapper : requiredModuleParameterClasses) {
            boolean found = false;

            // check at the module level first
            if (moduleParameterSetNames.keySet().contains(classWrapper)) {
                paramSetName = moduleParameterSetNames.get(classWrapper);
                found = true;
            } else if (pipelineParameterSetNames.keySet().contains(classWrapper)) {
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
                    BeanWrapper<Parameters> bean = paramSet.getParameters();
                    if (bean.hasNewUnsavedFields()) {
                        validationResults.addError(errorLabel + ": parameter set: " + paramSetName
                            + " has new fields that have been added since the last time this parameter set was saved.  "
                            + "Please edit the parameter set in the parameter library, verify that it has the correct values, and save it.");
                    }

                    // Validate the parameters.
                    try {
                        bean.getInstance().validate();
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
        for (ClassWrapper<Parameters> moduleParameterClass : moduleParameterSetNames.keySet()) {
            if (pipelineParameterSetNames.containsKey(moduleParameterClass)) {
                validationResults.addError(
                    "Ambiguous configuration: Module parameter and pipeline parameter Maps both contain a value for parameter class: "
                        + moduleParameterClass);
            }
        }
    }

    /**
     * @param parameterSetNames
     * @param errorLabel
     * @param validationResults
     */
    private void validateParameterClassExists(
        Map<ClassWrapper<Parameters>, ParameterSetName> parameterSetNames, String errorLabel,
        TriggerValidationResults validationResults) {
        for (ClassWrapper<Parameters> classWrapper : parameterSetNames.keySet()) {
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
     * Generate a text report containing pipeline instance metadata including all parameter sets and
     * their values.
     *
     * @param instance
     * @return
     */
    public String generatePedigreeReport(PipelineInstance instance) {
        PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();
        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        String nl = System.getProperty("line.separator");
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
        report.append("Definition ID: " + instance.getPipelineDefinition().getId() + nl);

        report.append(nl);
        report.append("Pipeline Parameter Sets" + nl);
        Map<ClassWrapper<Parameters>, ParameterSet> pipelineParamSets = instance
            .getPipelineParameterSets();
        for (ClassWrapper<Parameters> paramClassWrapper : pipelineParamSets.keySet()) {
            ParameterSet paramSet = pipelineParamSets.get(paramClassWrapper);

            appendParameterSetToReport(report, paramSet, "  ", false);
            report.append(nl);
        }

        report.append(nl);
        report.append("Modules" + nl);

        List<PipelineInstanceNode> pipelineNodes = pipelineInstanceNodeCrud.retrieveAll(instance);

        for (PipelineInstanceNode node : pipelineNodes) {
            PipelineModuleDefinition module = node.getPipelineModuleDefinition();

            appendModule(nl, report, module);

            report.append("    # Tasks (total/completed/failed): " + node.getNumTasks() + "/"
                + node.getNumCompletedTasks() + "/" + node.getNumFailedTasks() + nl);
            List<String> nodeSoftwareRevisions = pipelineTaskCrud.distinctSoftwareRevisions(node);
            report.append("    Software Revisions for node:" + nodeSoftwareRevisions + nl);

            Map<ClassWrapper<Parameters>, ParameterSet> moduleParamSets = node
                .getModuleParameterSets();
            for (ClassWrapper<Parameters> paramClassWrapper : moduleParamSets.keySet()) {
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
     *
     * @param pipelineDefinition
     * @return
     */
    public String generateTriggerReport(PipelineDefinition pipelineDefinition) {
        String nl = System.getProperty("line.separator");
        StringBuilder report = new StringBuilder();
        ParameterSetCrud paramSetCrud = new ParameterSetCrud();
        PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();

        report.append("Trigger ID: " + pipelineDefinition.getId() + nl);
        report.append("Trigger Name: " + pipelineDefinition.getName() + nl);
        report.append("Trigger Priority: " + pipelineDefinition.getInstancePriority() + nl);
        report.append(nl);

        TriggerValidationResults validationErrors = validateTrigger(pipelineDefinition);
        if (validationErrors.hasErrors()) {
            report.append("*** Trigger Validation Errors ***" + nl);
            report.append(nl);
            report.append(validationErrors.errorReport("  "));
            report.append(nl);
        }

        report.append("Definition Name: " + pipelineDefinition.getName().getName() + nl);

        report.append("Pipeline Parameter Sets" + nl);
        Map<ClassWrapper<Parameters>, ParameterSetName> pipelineParamSets = pipelineDefinition
            .getPipelineParameterSetNames();
        for (ClassWrapper<Parameters> paramClassWrapper : pipelineParamSets.keySet()) {
            ParameterSetName paramSetName = pipelineParamSets.get(paramClassWrapper);
            ParameterSet paramSet = paramSetCrud.retrieveLatestVersionForName(paramSetName);

            appendParameterSetToReport(report, paramSet, "  ", false);
            report.append(nl);
        }

        report.append(nl);
        report.append("Modules" + nl);

        List<PipelineDefinitionNode> nodes = pipelineDefinition.getNodes();
        for (PipelineDefinitionNode node : nodes) {
            ModuleName moduleName = node.getModuleName();

            PipelineModuleDefinition modDef = pipelineModuleDefinitionCrud
                .retrieveLatestVersionForName(moduleName);

            appendModule(nl, report, modDef);

            Map<ClassWrapper<Parameters>, ParameterSetName> moduleParamSetNames = node
                .getModuleParameterSetNames();
            for (ClassWrapper<Parameters> paramClassWrapper : moduleParamSetNames.keySet()) {
                ParameterSetName moduleParamSetName = moduleParamSetNames.get(paramClassWrapper);
                ParameterSet moduleParamSet = paramSetCrud
                    .retrieveLatestVersionForName(moduleParamSetName);

                appendParameterSetToReport(report, moduleParamSet, "    ", false);
                report.append(nl);
            }
        }

        return report.toString();
    }

    private void appendModule(String nl, StringBuilder report, PipelineModuleDefinition module) {
        report.append(nl);
        report.append(
            "  Module Definition: " + module.getName() + ", version=" + module.getVersion() + nl);
        report.append("    Java Classname: "
            + module.getPipelineModuleClass().getClazz().getSimpleName() + nl);
        report.append("    exe timeout seconds: " + module.getExeTimeoutSecs() + nl);
        report.append("    min memory MB: " + module.getMinMemoryMegaBytes() + nl);
    }

    /**
     * @param pipelineDefinition
     */
    public void exportPipelineParams(PipelineDefinition pipelineDefinition,
        File destinationDirectory) {
        if (!destinationDirectory.exists()) {
            try {
                FileUtils.forceMkdir(destinationDirectory);
            } catch (IOException e) {
                throw new PipelineException(
                    "failed to create [" + destinationDirectory + "], caught e = " + e, e);
            }
        }

        ParameterSetCrud paramSetCrud = new ParameterSetCrud();
        Map<String, Parameters> paramsToExport = new HashMap<>();

        Map<ClassWrapper<Parameters>, ParameterSetName> pipelineParamSets = pipelineDefinition
            .getPipelineParameterSetNames();
        for (ClassWrapper<Parameters> paramClassWrapper : pipelineParamSets.keySet()) {
            ParameterSetName paramSetName = pipelineParamSets.get(paramClassWrapper);
            ParameterSet paramSet = paramSetCrud.retrieveLatestVersionForName(paramSetName);

            paramsToExport.put(paramSetName.getName(), paramSet.parametersInstance());
        }

        List<PipelineDefinitionNode> nodes = pipelineDefinition.getNodes();
        for (PipelineDefinitionNode node : nodes) {
            Map<ClassWrapper<Parameters>, ParameterSetName> moduleParamSetNames = node
                .getModuleParameterSetNames();
            for (ClassWrapper<Parameters> paramClassWrapper : moduleParamSetNames.keySet()) {
                ParameterSetName moduleParamSetName = moduleParamSetNames.get(paramClassWrapper);
                ParameterSet moduleParamSet = paramSetCrud
                    .retrieveLatestVersionForName(moduleParamSetName);

                paramsToExport.put(moduleParamSetName.getName(),
                    moduleParamSet.parametersInstance());
            }
        }

        for (String paramSetName : paramsToExport.keySet()) {
            Parameters params = paramsToExport.get(paramSetName);
            File file = new File(destinationDirectory, paramSetName + ".properties");

            try {
                ParametersUtils.exportParameters(file, params);
            } catch (IOException e) {
                throw new PipelineException("failed to export [" + file + "], caught e = " + e, e);
            }
        }
    }

    /**
     * Creates a textual report of all ParameterSets in the Parameter Library, including name, type,
     * keys and values.
     *
     * @param csvMode
     * @return
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
     *
     * @param report
     * @param paramSet
     * @param indent
     */
    public void appendParameterSetToReport(StringBuilder report, ParameterSet paramSet,
        String indent, boolean csvMode) {
        String nl = System.getProperty("line.separator");
        String paramsIndent = indent + "  ";
        BeanWrapper<Parameters> parameters = paramSet.getParameters();
        String parameterClassName = "";

        try {
            parameterClassName = parameters.getClazz().getSimpleName();
        } catch (RuntimeException e) {
            parameterClassName = " <deleted>: " + parameters.getClassName();
        }

        if (!csvMode) {
            report.append(indent + "Parameter Set: " + paramSet.getName() + " (type="
                + parameterClassName + ", version=" + paramSet.getVersion() + ")" + nl);
        }

        Map<String, TypedParameter> params = BeanWrapper
            .typedPropertyByName(parameters.getTypedProperties());
        if (params.isEmpty() && !csvMode) {
            report.append(paramsIndent + "(no parameters)" + nl);
        } else {
            List<String> sortedKeys = new LinkedList<>(params.keySet());
            Collections.sort(sortedKeys);
            for (String key : sortedKeys) {
                String value = params.get(key).getString();

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

    public Map<ClassWrapper<Parameters>, ParameterSet> retrieveParameterSets(
        PipelineDefinition pipelineDefinition, String moduleName) {
        Map<ClassWrapper<Parameters>, ParameterSetName> parameterSetNameMap = new HashMap<>();
        parameterSetNameMap.putAll(pipelineDefinition.getPipelineParameterSetNames());

        for (PipelineDefinitionNode pipelineDefinitionNode : pipelineDefinition.getNodes()) {
            if (pipelineDefinitionNode.getModuleName().getName().equals(moduleName)) {
                parameterSetNameMap.putAll(pipelineDefinitionNode.getModuleParameterSetNames());
            }
        }

        ParameterSetCrud parameterSetCrud = new ParameterSetCrud();
        Map<ClassWrapper<Parameters>, ParameterSet> parameterSetMap = new HashMap<>();
        for (Entry<ClassWrapper<Parameters>, ParameterSetName> entry : parameterSetNameMap
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
}
