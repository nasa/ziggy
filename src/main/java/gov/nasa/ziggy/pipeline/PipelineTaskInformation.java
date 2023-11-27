package gov.nasa.ziggy.pipeline;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.SubtaskInformation;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.ParameterSetCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineModuleDefinitionCrud;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Generates and stores information about the {@link PipelineTask} instances for a particular
 * {@link PipelineDefinition} on a particular {@link PipelineDefinitionNode}. Each task required for
 * the execution of the trigger is stored, along with the {@link SubtaskInformation} instance for
 * each task. The {@link ParameterSet} for the node's {@link RemoteParameters} instance is stored,
 * if any such exists.
 * <p>
 * The class provides static methods that can be used to retrieve or delete information on pipeline
 * tasks based on a given {@link PipelineDefinitionNode}. A singleton instance of
 * {@link PipelineTaskInformation} is used as a container for various CRUD classes, which supports
 * unit testing.
 *
 * @author PT
 */
public class PipelineTaskInformation {

    private static final Logger log = LoggerFactory.getLogger(PipelineTaskInformation.class);

    private ParameterSetCrud parameterSetCrud;
    private PipelineDefinitionCrud pipelineDefinitionCrud;
    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;

    void setParameterSetCrud(ParameterSetCrud parameterSetCrud) {
        this.parameterSetCrud = parameterSetCrud;
    }

    void setPipelineDefinitionCrud(PipelineDefinitionCrud pipelineDefinitionCrud) {
        this.pipelineDefinitionCrud = pipelineDefinitionCrud;
    }

    void setPipelineModuleDefinitionCrud(
        PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud) {
        this.pipelineModuleDefinitionCrud = pipelineModuleDefinitionCrud;
    }

    private ParameterSetCrud getParameterSetCrud() {
        if (parameterSetCrud == null) {
            parameterSetCrud = new ParameterSetCrud();
        }
        return parameterSetCrud;
    }

    private PipelineDefinitionCrud getPipelineDefinitionCrud() {
        if (pipelineDefinitionCrud == null) {
            pipelineDefinitionCrud = new PipelineDefinitionCrud();
        }
        return pipelineDefinitionCrud;
    }

    private PipelineModuleDefinitionCrud getPipelineModuleDefinitionCrud() {
        if (pipelineModuleDefinitionCrud == null) {
            pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        }
        return pipelineModuleDefinitionCrud;
    }

    /**
     * Singleton instance.
     */
    private static PipelineTaskInformation instance = new PipelineTaskInformation();

    /**
     * Allows a mocked instance to be provided for unit tests.
     */
    static void setInstance(PipelineTaskInformation newInstance) {
        instance = newInstance;
    }

    /**
     * Cache of information organized by {@link PipelineDefinitionNode}.
     */
    private static Map<PipelineDefinitionNode, List<SubtaskInformation>> subtaskInformationMap = new HashMap<>();

    /**
     * Cache of {@link ParameterSetName}s for {@link RemoteParameters} instances, organized by
     * {@link PipelineDefinitionNode}.
     */
    private static Map<PipelineDefinitionNode, String> remoteParametersMap = new HashMap<>();

    /**
     * Cache that stores information on whether a given module has limits on the number of subtasks
     * that can be processed in parallel.
     */
    private static Map<PipelineDefinitionNode, Boolean> modulesWithParallelLimitsMap = new HashMap<>();

    /**
     * Deletes the cached information for a given {@link PipelineDefinitionNode}. Used when the user
     * is aware of changes that should force recalculation.
     */
    public synchronized static void reset(PipelineDefinitionNode triggerDefinitionNode) {
        if (subtaskInformationMap.containsKey(triggerDefinitionNode)) {
            subtaskInformationMap.put(triggerDefinitionNode, null);
        }
        if (remoteParametersMap.containsKey(triggerDefinitionNode)) {
            remoteParametersMap.remove(triggerDefinitionNode);
        }
        if (modulesWithParallelLimitsMap.containsKey(triggerDefinitionNode)) {
            remoteParametersMap.remove(triggerDefinitionNode);
        }
    }

    public synchronized static boolean hasPipelineDefinitionNode(PipelineDefinitionNode node) {
        return subtaskInformationMap.containsKey(node) && subtaskInformationMap.get(node) != null
            && !subtaskInformationMap.get(node).isEmpty();
    }

    /**
     * Returns the {@link List} of {@link SubtaskInformation} for the specified
     * {@link PipelineDefinitionNode}. If there is no such list in the cache, the information is
     * generated, cached, and returned.
     */
    public static synchronized List<SubtaskInformation> subtaskInformation(
        PipelineDefinitionNode node) {
        if (!hasPipelineDefinitionNode(node)) {
            generateSubtaskInformation(node);
        }
        return subtaskInformationMap.get(node);
    }

    /**
     * Returns the name of the {@link ParameterSet} for a specified node's {@link RemoteParameters}
     * instance. If the module has no such parameter set, null is returned.
     */
    public static synchronized String remoteParameters(PipelineDefinitionNode node) {
        if (!hasPipelineDefinitionNode(node)) {
            generateSubtaskInformation(node);
        }
        return remoteParametersMap.get(node);
    }

    /**
     * Determines whether a given {@link PipelineDefinitionNode} corresponds to a module that limits
     * the maximum number of subtasks that can be processed in parallel (this is usually the case
     * for a module that is forced to perform its processing in multiple steps, where each step
     * processes a unique set of subtasks).
     */
    public static synchronized boolean parallelLimits(PipelineDefinitionNode node) {
        if (!hasPipelineDefinitionNode(node)) {
            generateSubtaskInformation(node);
        }
        return modulesWithParallelLimitsMap.get(node);
    }

    /**
     * Calculation engine that generates the {@link List} of {@link SubtaskInformation} instances
     * for a given {@link PipelineDefinitionNode}. The calculation first generates the
     * {@link UnitOfWorkTask} instances for the given node. These are used to construct
     * {@link PipelineTask} instances for each unit of work. Finally, the subtask information method
     * in the appropriate {@link PipelineInputs} subclasss is used to generate an instance of
     * {@link SubtaskInformation} for each pipeline task.
     */

    private static synchronized void generateSubtaskInformation(PipelineDefinitionNode node) {

        log.debug("Generating subtask information for node " + node.getModuleName());
        PipelineDefinition pipelineDefinition = pipelineDefinitionCrud()
            .retrieveLatestVersionForName(node.getPipelineName());

        // Construct the pipeline instance.
        PipelineInstance pipelineInstance = new PipelineInstance();
        pipelineInstance.setPipelineDefinition(pipelineDefinition);

        // Populate the instance parameters
        Map<ClassWrapper<ParametersInterface>, String> triggerParamNames = pipelineDefinition
            .getPipelineParameterSetNames();
        Map<ClassWrapper<ParametersInterface>, ParameterSet> instanceParams = pipelineInstance
            .getPipelineParameterSets();
        populateParameters(triggerParamNames, instanceParams);
        pipelineInstance.setPipelineParameterSets(instanceParams);

        // Find the pipeline definition node of interest
        PipelineModuleDefinition moduleDefinition = pipelineModuleDefinitionCrud()
            .retrieveLatestVersionForName(node.getModuleName());

        // Construct a PipelineInstanceNode for this module
        PipelineInstanceNode instanceNode = new PipelineInstanceNode(pipelineInstance, node,
            moduleDefinition);
        triggerParamNames = node.getModuleParameterSetNames();
        instanceParams = instanceNode.getModuleParameterSets();
        populateParameters(triggerParamNames, instanceParams);
        instanceNode.setModuleParameterSets(instanceParams);

        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = instance.unitOfWorkGenerator(node);

        // Produce a combined map from Parameter classes to Parameter instances
        Map<ClassWrapper<ParametersInterface>, ParameterSet> compositeParameterSets = new HashMap<>(
            pipelineInstance.getPipelineParameterSets());

        for (ClassWrapper<ParametersInterface> moduleParameterClass : instanceNode
            .getModuleParameterSets()
            .keySet()) {
            if (compositeParameterSets.containsKey(moduleParameterClass)) {
                throw new PipelineException(
                    "Configuration Error: Module parameter and pipeline parameter Maps both contain a value for parameter class: "
                        + moduleParameterClass);
            }
            compositeParameterSets.put(moduleParameterClass,
                instanceNode.getModuleParameterSets().get(moduleParameterClass));
        }

        // Set the flag that indicates whether this module limits the number of subtasks that
        // can run in parallel

        modulesWithParallelLimitsMap.put(node, instance.parallelLimits(moduleDefinition));
        Map<Class<? extends ParametersInterface>, ParametersInterface> uowParams = new HashMap<>();

        for (ClassWrapper<ParametersInterface> parametersClass : compositeParameterSets.keySet()) {
            ParameterSet parameterSet = compositeParameterSets.get(parametersClass);
            Class<? extends ParametersInterface> clazz = parametersClass.getClazz();
            if (clazz.equals(RemoteParameters.class)) {
                remoteParametersMap.put(node, parameterSet.getName());
            }
            uowParams.put(clazz, parameterSet.parametersInstance());
        }
        if (!remoteParametersMap.containsKey(node)) {
            remoteParametersMap.put(node, null);
        }

        // Generate the units of work.
        List<UnitOfWork> tasks = instance.unitsOfWork(unitOfWorkGenerator, uowParams);

        // Generate the subtask information for all tasks
        List<SubtaskInformation> subtaskInformationList = new LinkedList<>();
        for (UnitOfWork task : tasks) {
            PipelineTask pipelineTask = instance.pipelineTask(pipelineInstance, instanceNode, task);
            pipelineTask.setUowTaskParameters(task.getParameters());
            SubtaskInformation subtaskInformation = instance.subtaskInformation(moduleDefinition,
                pipelineTask);
            subtaskInformationList.add(subtaskInformation);
        }
        subtaskInformationMap.put(node, subtaskInformationList);
    }

    /**
     * Generates the {@link UnitOfWork} instances. Implemented as an instance method in support of
     * unit tests.
     */
    List<UnitOfWork> unitsOfWork(ClassWrapper<UnitOfWorkGenerator> wrappedUowGenerator,
        Map<Class<? extends ParametersInterface>, ParametersInterface> uowParams) {
        UnitOfWorkGenerator taskGenerator = wrappedUowGenerator.newInstance();
        return taskGenerator.generateUnitsOfWork(uowParams);
    }

    /**
     * Retrieves the {@link UnitOfWorkGenerator} for the task. Implemented as an instance method in
     * support of unit tests.
     */
    ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineDefinitionNode node) {
        return UnitOfWorkGenerator.unitOfWorkGenerator(node);
    }

    /**
     * Generates a {@link PipelineTask}. Implemented as an instance method in support of unit tests.
     */
    PipelineTask pipelineTask(PipelineInstance instance, PipelineInstanceNode instanceNode,
        UnitOfWork uow) {
        PipelineTask pipelineTask = new PipelineTask(instance, instanceNode);
        pipelineTask.setUowTaskParameters(uow.getParameters());
        return pipelineTask;
    }

    boolean parallelLimits(PipelineModuleDefinition moduleDefinition) {
        PipelineInputs pipelineInputs = moduleDefinition.getInputsClass().newInstance();
        return pipelineInputs.parallelLimits();
    }

    /**
     * Generates the {@link SubtaskInformation} instance for a single {@link PipelineTask}.
     * Implemented as an instance method in support of unit tests.
     */
    SubtaskInformation subtaskInformation(PipelineModuleDefinition moduleDefinition,
        PipelineTask pipelineTask) {
        PipelineInputs pipelineInputs = moduleDefinition.getInputsClass().newInstance();
        return pipelineInputs.subtaskInformation(pipelineTask);
    }

    private static void populateParameters(
        Map<ClassWrapper<ParametersInterface>, String> parameterSetNames,
        Map<ClassWrapper<ParametersInterface>, ParameterSet> parameterSets) {
        for (ClassWrapper<ParametersInterface> paramClass : parameterSetNames.keySet()) {
            String pipelineParamName = parameterSetNames.get(paramClass);
            ParameterSet paramSet = parameterSetCrud()
                .retrieveLatestVersionForName(pipelineParamName);
            parameterSets.put(paramClass, paramSet);
        }
    }

    // Methods to retrieve CRUD instance from the singleton.
    private static synchronized ParameterSetCrud parameterSetCrud() {
        return instance.getParameterSetCrud();
    }

    private static synchronized PipelineDefinitionCrud pipelineDefinitionCrud() {
        return instance.getPipelineDefinitionCrud();
    }

    private static synchronized PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud() {
        return instance.getPipelineModuleDefinitionCrud();
    }
}
