package gov.nasa.ziggy.pipeline;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.PipelineInputsOutputsUtils;
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
     * Deletes the cached information for a given {@link PipelineDefinitionNode}. Used when the user
     * is aware of changes that should force recalculation.
     */
    public synchronized static void reset(PipelineDefinitionNode triggerDefinitionNode) {
        if (subtaskInformationMap.containsKey(triggerDefinitionNode)) {
            subtaskInformationMap.put(triggerDefinitionNode, null);
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

        // Generate the units of work.
        List<UnitOfWork> tasks = instance.unitsOfWork(unitOfWorkGenerator, instanceNode,
            pipelineInstance);

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
        PipelineInstanceNode pipelineInstanceNode, PipelineInstance pipelineInstance) {
        UnitOfWorkGenerator taskGenerator = wrappedUowGenerator.newInstance();
        return PipelineExecutor.generateUnitsOfWork(taskGenerator, pipelineInstanceNode,
            pipelineInstance);
    }

    /**
     * Retrieves the {@link UnitOfWorkGenerator} for the task. Implemented as an instance method in
     * support of unit tests.
     */
    ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineDefinitionNode node) {
        return pipelineModuleDefinitionCrud().retrieveUnitOfWorkGenerator(node.getModuleName());
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

    /**
     * Generates the {@link SubtaskInformation} instance for a single {@link PipelineTask}.
     * Implemented as an instance method in support of unit tests.
     */
    SubtaskInformation subtaskInformation(PipelineModuleDefinition moduleDefinition,
        PipelineTask pipelineTask) {
        PipelineInputs pipelineInputs = PipelineInputsOutputsUtils
            .newPipelineInputs(moduleDefinition.getInputsClass(), pipelineTask, null);
        return pipelineInputs.subtaskInformation();
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
