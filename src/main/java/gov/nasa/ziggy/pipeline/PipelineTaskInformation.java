package gov.nasa.ziggy.pipeline;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineStepOperations;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputs;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputsOutputsUtils;
import gov.nasa.ziggy.pipeline.step.subtask.SubtaskInformation;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;

/**
 * Generates and stores information about the {@link PipelineTask} instances for a particular
 * {@link Pipeline} on a particular {@link PipelineNode}. Each task required for the execution of
 * the pipeline is stored, along with the {@link SubtaskInformation} instance for each task.
 * <p>
 * The class provides static methods that can be used to retrieve or delete information on pipeline
 * tasks based on a given {@link PipelineNode}. A singleton instance of
 * {@link PipelineTaskInformation} is used as a container for various CRUD classes, which supports
 * unit testing.
 *
 * @author PT
 */
public class PipelineTaskInformation {

    private static final Logger log = LoggerFactory.getLogger(PipelineTaskInformation.class);

    private PipelineOperations pipelineOperations = new PipelineOperations();
    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();

    /**
     * Singleton instance.
     */
    private static PipelineTaskInformation instance = new PipelineTaskInformation();

    /**
     * Cache of information organized by {@link PipelineNode}.
     */
    private static Map<PipelineNode, List<SubtaskInformation>> subtaskInformationMap = new HashMap<>();

    /**
     * Deletes the cached information for a given {@link PipelineNode}. Used when the user is aware
     * of changes that should force recalculation.
     */
    public synchronized static void reset(PipelineNode pipelineNode) {
        if (subtaskInformationMap.containsKey(pipelineNode)) {
            subtaskInformationMap.put(pipelineNode, null);
        }
    }

    public synchronized static boolean hasPipelineNode(PipelineNode node) {
        return subtaskInformationMap.containsKey(node) && subtaskInformationMap.get(node) != null
            && !subtaskInformationMap.get(node).isEmpty();
    }

    /**
     * Returns the {@link List} of {@link SubtaskInformation} for the specified
     * {@link PipelineNode}. If there is no such list in the cache, the information is generated,
     * cached, and returned.
     */
    public static synchronized List<SubtaskInformation> subtaskInformation(PipelineNode node) {
        if (!hasPipelineNode(node)) {
            generateSubtaskInformation(node);
        }
        return subtaskInformationMap.get(node);
    }

    /**
     * Calculation engine that generates the {@link List} of {@link SubtaskInformation} instances
     * for a given {@link PipelineNode}. The calculation first generates the {@link UnitOfWorkTask}
     * instances for the given node. These are used to construct {@link PipelineTask} instances for
     * each unit of work. Finally, the subtask information method in the appropriate
     * {@link PipelineInputs} subclasss is used to generate an instance of
     * {@link SubtaskInformation} for each pipeline task.
     */
    private static synchronized void generateSubtaskInformation(PipelineNode node) {

        log.debug("Generating subtask information for node {}...", node.getPipelineStepName());
        Pipeline pipeline = instancePipelineOperations().pipeline(node.getPipelineName());

        // Construct the pipeline instance.
        PipelineInstance pipelineInstance = new PipelineInstance();
        pipelineInstance.setPipeline(pipeline);

        // Find the pipeline step of interest.
        PipelineStep pipelineStep = instancePipelineStepOperations()
            .pipelineStep(node.getPipelineStepName());

        // Construct a PipelineInstanceNode for this node.
        PipelineInstanceNode instanceNode = new PipelineInstanceNode(node, pipelineStep);

        ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator = instance.unitOfWorkGenerator(node);

        // Generate the units of work.
        List<UnitOfWork> unitsOfWork = instance.unitsOfWork(unitOfWorkGenerator, instanceNode,
            pipelineInstance);

        // Generate the subtask information for all tasks.
        List<SubtaskInformation> subtaskInformationList = new LinkedList<>();
        for (UnitOfWork unitOfWork : unitsOfWork) {
            PipelineTask pipelineTask = instance.pipelineTask(pipelineInstance, instanceNode,
                unitOfWork);
            SubtaskInformation subtaskInformation = instance.subtaskInformation(pipelineStep,
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
    ClassWrapper<UnitOfWorkGenerator> unitOfWorkGenerator(PipelineNode node) {
        return pipelineStepOperations().unitOfWorkGenerator(node.getPipelineStepName());
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
    SubtaskInformation subtaskInformation(PipelineStep pipelineStep, PipelineTask pipelineTask,
        PipelineNode pipelineNode) {
        return PipelineInputsOutputsUtils
            .newPipelineInputs(pipelineStep.getInputsClass(), pipelineTask, null)
            .subtaskInformation(pipelineNode);
    }

    /**
     * Allows a mocked instance to be provided for unit tests.
     */
    static void setInstance(PipelineTaskInformation newInstance) {
        instance = newInstance;
    }

    PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }

    PipelineStepOperations pipelineStepOperations() {
        return pipelineStepOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    PipelineInstanceNodeOperations pipelineInstanceNodeOperations() {
        return pipelineInstanceNodeOperations;
    }

    private static synchronized PipelineOperations instancePipelineOperations() {
        return instance.pipelineOperations();
    }

    private static synchronized PipelineStepOperations instancePipelineStepOperations() {
        return instance.pipelineStepOperations();
    }
}
