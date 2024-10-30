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
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.ParametersOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineModuleDefinitionOperations;
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

    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

    /**
     * Singleton instance.
     */
    private static PipelineTaskInformation instance = new PipelineTaskInformation();

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

        log.debug("Generating subtask information for node {}", node.getModuleName());
        PipelineDefinition pipelineDefinition = instancePipelineDefinitionOperations()
            .pipelineDefinition(node.getPipelineName());

        // Construct the pipeline instance.
        PipelineInstance pipelineInstance = new PipelineInstance();
        pipelineInstance.setPipelineDefinition(pipelineDefinition);

        // Populate the instance parameters
        pipelineInstance
            .setParameterSets(instanceParametersOperations().parameterSets(pipelineDefinition));

        // Find the pipeline definition node of interest
        PipelineModuleDefinition moduleDefinition = instancePipelineModuleDefinitionOperations()
            .pipelineModuleDefinition(node.getModuleName());

        // Construct a PipelineInstanceNode for this module
        PipelineInstanceNode instanceNode = new PipelineInstanceNode(node, moduleDefinition);
        instanceNode.setParameterSets(instanceParametersOperations().parameterSets(node));

        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = instance.unitOfWorkGenerator(node);

        // Generate the units of work.
        List<UnitOfWork> unitsOfWork = instance.unitsOfWork(unitOfWorkGenerator, instanceNode,
            pipelineInstance);

        // Generate the subtask information for all tasks
        List<SubtaskInformation> subtaskInformationList = new LinkedList<>();
        for (UnitOfWork unitOfWork : unitsOfWork) {
            PipelineTask pipelineTask = instance.pipelineTask(pipelineInstance, instanceNode,
                unitOfWork);
            SubtaskInformation subtaskInformation = instance.subtaskInformation(moduleDefinition,
                pipelineTask, node);
            subtaskInformationList.add(subtaskInformation);
        }
        subtaskInformationMap.put(node, subtaskInformationList);
        log.debug("Generating subtask information...done");
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
        return pipelineModuleDefinitionOperations().unitOfWorkGenerator(node.getModuleName());
    }

    /**
     * Generates a {@link PipelineTask}. Implemented as an instance method in support of unit tests.
     */
    PipelineTask pipelineTask(PipelineInstance instance, PipelineInstanceNode instanceNode,
        UnitOfWork uow) {
        return new PipelineTask(instance, instanceNode, uow);
    }

    /**
     * Generates the {@link SubtaskInformation} instance for a single {@link PipelineTask}.
     * Implemented as an instance method in support of unit tests.
     */
    SubtaskInformation subtaskInformation(PipelineModuleDefinition moduleDefinition,
        PipelineTask pipelineTask, PipelineDefinitionNode pipelineDefinitionNode) {
        PipelineInputs pipelineInputs = PipelineInputsOutputsUtils
            .newPipelineInputs(moduleDefinition.getInputsClass(), pipelineTask, null);
        return pipelineInputs.subtaskInformation(pipelineDefinitionNode);
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    /**
     * Allows a mocked instance to be provided for unit tests.
     */
    static void setInstance(PipelineTaskInformation newInstance) {
        instance = newInstance;
    }

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations() {
        return pipelineModuleDefinitionOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    private static synchronized PipelineDefinitionOperations instancePipelineDefinitionOperations() {
        return instance.pipelineDefinitionOperations();
    }

    private static synchronized PipelineModuleDefinitionOperations instancePipelineModuleDefinitionOperations() {
        return instance.pipelineModuleDefinitionOperations();
    }

    private static synchronized ParametersOperations instanceParametersOperations() {
        return instance.parametersOperations();
    }
}
