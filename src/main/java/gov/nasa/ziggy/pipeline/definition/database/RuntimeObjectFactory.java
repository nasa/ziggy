package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.Pipeline;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.worker.WorkerResources;
import gov.nasa.ziggy.worker.WorkerResourcesOperations;

/**
 * Provides methods to construct new objects of the {@link PipelineInstance},
 * {@link PipelineInstanceNode}, and {@link PipelineTask} classes.
 * <p>
 * The runtime classes require numerous relationships with one another and with the pipeline classes
 * ({@link Pipeline}, {@link PipelineNode}, {@link PipelineStep}). For this reason, construction of
 * new objects of the runtime classes is complex and requires interacting with instances of multiple
 * other classes. {@link RuntimeObjectFactory} provides methods that produce those instances and
 * correctly update all relationships.
 *
 * @author PT
 */
public class RuntimeObjectFactory extends DatabaseOperations {

    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineNodeOperations pipelineNodeOperations = new PipelineNodeOperations();
    private PipelineStepOperations pipelineStepOperations = new PipelineStepOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private PipelineOperations pipelineOperations = new PipelineOperations();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();
    private WorkerResourcesOperations workerResourcesOperations = new WorkerResourcesOperations();

    /**
     * Creates {@link PipelineInstanceNode} instances for the initial {@link PipelineNode}s of a
     * given pipeline run. Pipeline instance nodes are also created for all subsequent pipeline
     * nodes in the run and stored in the database; however, only the instance nodes for the
     * starting pipeline nodes are returned. This signature is used when starting at the first node
     * or nodes that are to be run in the given pipeline instance, so there is no parent instance
     * node.
     */
    public List<PipelineInstanceNode> newInstanceNodes(Pipeline pipeline,
        PipelineInstance pipelineInstance, List<PipelineNode> startNodes, PipelineNode endNode) {
        return newInstanceNodes(pipeline, pipelineInstance, startNodes, endNode, null);
    }

    /**
     * Creates {@link PipelineInstanceNode} instances for the initial {@link PipelineNode}s of a
     * given pipeline run. Pipeline instance nodes are also created for all subsequent pipeline
     * nodes in the run and stored in the database; however, only the instance nodes for the
     * starting pipeline nodes are returned.
     */
    private List<PipelineInstanceNode> newInstanceNodes(Pipeline pipeline,
        PipelineInstance pipelineInstance, List<PipelineNode> pipelineNodes, PipelineNode endNode,
        PipelineInstanceNode parentInstanceNode) {

        Map<PipelineNode, List<PipelineNode>> nextNodesByCurrentNode = new HashMap<>();
        Map<PipelineNode, PipelineInstanceNode> instanceNodeByPipelineNode = new HashMap<>();

        // Note that all the following takes place in one transaction. Although the operations
        // class methods nominally have their own transaction boundaries, in the case of nested
        // transaction the "inner transaction" is treated as part of the "outer" one by simply
        // ignoring the start, commit, and rollback calls in the "inner transaction."
        List<PipelineInstanceNode> pipelineInstanceNodes = performTransaction(() -> {

            List<PipelineInstanceNode> instanceNodes = new ArrayList<>();
            for (PipelineNode pipelineNode : pipelineNodes) {

                // Update the Map from parent to child nodes.
                nextNodesByCurrentNode.put(pipelineNode,
                    pipelineNodeOperations().nextNodes(pipelineNode));

                // Get the pipeline step and lock it.
                PipelineStep pipelineStep = pipelineStepOperations()
                    .pipelineStep(pipelineNode.getPipelineStepName());
                pipelineStepOperations().lock(pipelineStep);

                // Construct the new pipeline instance node and put it into the Map.
                PipelineInstanceNode instanceNode = new PipelineInstanceNode(pipelineNode,
                    pipelineStep);

                // Combine the pipeline node resources with the default values; this is
                // the set of worker resources for this pipeline instance node.
                WorkerResources compositeWorkerResources = workerResourcesOperations
                    .compositeWorkerResources(pipelineNode);
                instanceNode.setMaxWorkerCount(compositeWorkerResources.getMaxWorkerCount());
                instanceNode.setHeapSizeGigabytes(compositeWorkerResources.getHeapSizeGigabytes());
                instanceNode = pipelineInstanceNodeOperations().merge(instanceNode);
                instanceNodeByPipelineNode.put(pipelineNode, instanceNode);

                if (parentInstanceNode == null) {
                    pipelineInstanceOperations().addRootNode(pipelineInstance, instanceNode);
                } else {
                    pipelineInstanceOperations().addPipelineInstanceNode(pipelineInstance,
                        instanceNode);
                }

                // Bind the node parameter sets to the instance node.
                pipelineInstanceNodeOperations().bindParameterSets(pipelineNode, instanceNode);
                instanceNodes.add(instanceNode);
            }
            if (parentInstanceNode != null) {
                pipelineInstanceNodeOperations().addNextNodes(parentInstanceNode, instanceNodes);
            }
            return instanceNodes;
        });

        // Recursively create instance nodes for the child nodes of each pipeline
        // node, unless the node in question is the end node. Note that we need to create these
        // but do not need to return them.
        for (Map.Entry<PipelineNode, List<PipelineNode>> entry : nextNodesByCurrentNode
            .entrySet()) {
            if (endNode != null && entry.getKey().getId().longValue() == endNode.getId().longValue()
                || entry.getValue().isEmpty()) {
                continue;
            }
            newInstanceNodes(pipeline, pipelineInstance, entry.getValue(), endNode,
                instanceNodeByPipelineNode.get(entry.getKey()));
        }
        return pipelineInstanceNodes;
    }

    /** Creates a new pipeline instance in state PROCESSING with its execution clock started. */
    public PipelineInstance newPipelineInstance(String name, Pipeline pipeline,
        ModelRegistry modelRegistry) {
        return performTransaction(() -> {
            PipelineInstance instance = new PipelineInstance(pipeline);
            instance.setName(name);
            instance.setPipeline(pipeline);
            pipelineOperations().lock(pipeline);
            instance.setState(PipelineInstance.State.PROCESSING);
            instance.startExecutionClock();
            instance.setPriority(pipeline.getInstancePriority());
            instance.setModelRegistry(modelRegistry);
            instance = pipelineInstanceOperations().merge(instance);
            pipelineInstanceOperations().bindParameterSets(pipeline, instance);
            return instance;
        });
    }

    /**
     * Creates new {@link PipelineTask}s for a given {@link PipelineInstanceNode} that are in step
     * WAITING_TO_RUN.
     */
    public List<PipelineTask> newPipelineTasks(PipelineInstanceNode instanceNode,
        PipelineInstance instance, List<UnitOfWork> unitsOfWork) {
        List<PipelineTask> pipelineTasks = new ArrayList<>();
        for (UnitOfWork unitOfWork : unitsOfWork) {
            PipelineTask pipelineTask = new PipelineTask(instance, instanceNode, unitOfWork);
            pipelineTask = pipelineTaskOperations().merge(pipelineTask);
            pipelineTaskDataOperations().createPipelineTaskData(pipelineTask,
                ProcessingStep.WAITING_TO_RUN);
            pipelineTasks.add(pipelineTask);
        }
        pipelineInstanceNodeOperations().addPipelineTasks(instanceNode, pipelineTasks);
        return pipelineTasks;
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    PipelineNodeOperations pipelineNodeOperations() {
        return pipelineNodeOperations;
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

    PipelineOperations pipelineOperations() {
        return pipelineOperations;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDataOperations pipelineTaskDataOperations() {
        return pipelineTaskDataOperations;
    }

    WorkerResourcesOperations workerResourcesOperations() {
        return workerResourcesOperations;
    }
}
