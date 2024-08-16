package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * Provides methods to construct new objects of the {@link PipelineInstance},
 * {@link PipelineInstanceNode}, and {@link PipelineTask} classes.
 * <p>
 * The runtime classes require numerous relationships with one another and with the pipeline
 * definition classes ({@link PipelineDefinition}, {@link PipelineDefinitionNode},
 * {@link PipelineModuleDefinition}). For this reason, construction of new objects of the runtime
 * classes is complex and requires interacting with instances of multiple other classes.
 * {@link RuntimeObjectFactory} provides methods that produce those instances and correctly update
 * all relationships.
 *
 * @author PT
 */
public class RuntimeObjectFactory extends DatabaseOperations {

    private ParametersOperations parametersOperations = new ParametersOperations();
    private PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations = new PipelineDefinitionNodeOperations();
    private PipelineModuleDefinitionOperations pipelineModuleDefinitionOperations = new PipelineModuleDefinitionOperations();
    private PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();
    private PipelineInstanceNodeOperations pipelineInstanceNodeOperations = new PipelineInstanceNodeOperations();
    private PipelineDefinitionOperations pipelineDefinitionOperations = new PipelineDefinitionOperations();
    private PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();

    /**
     * Creates {@link PipelineInstanceNode} instances for the initial
     * {@link PipelineDefinitionNode}s of a given pipeline run. Pipeline instance nodes are also
     * created for all subsequent pipeline definition nodes in the run and stored in the database;
     * however, only the instance nodes for the starting pipeline definition nodes are returned.
     * This signature is used when starting at the first node or nodes that are to be run in the
     * given pipeline instance, so there is no parent instance node.
     */
    public List<PipelineInstanceNode> newInstanceNodes(PipelineDefinition pipelineDefinition,
        PipelineInstance pipelineInstance, List<PipelineDefinitionNode> startNodes,
        PipelineDefinitionNode endNode) {
        return newInstanceNodes(pipelineDefinition, pipelineInstance, startNodes, endNode, null);
    }

    /**
     * Creates {@link PipelineInstanceNode} instances for the initial
     * {@link PipelineDefinitionNode}s of a given pipeline run. Pipeline instance nodes are also
     * created for all subsequent pipeline definition nodes in the run and stored in the database;
     * however, only the instance nodes for the starting pipeline definition nodes are returned.
     */
    private List<PipelineInstanceNode> newInstanceNodes(PipelineDefinition pipelineDefinition,
        PipelineInstance pipelineInstance, List<PipelineDefinitionNode> pipelineDefinitionNodes,
        PipelineDefinitionNode endNode, PipelineInstanceNode parentInstanceNode) {

        Map<PipelineDefinitionNode, List<PipelineDefinitionNode>> nextNodesByCurrentNode = new HashMap<>();
        Map<PipelineDefinitionNode, PipelineInstanceNode> instanceNodeByPipelineDefinitionNode = new HashMap<>();

        // Note that all the following takes place in one transaction. Although the operations
        // class methods nominally have their own transaction boundaries, in the case of nested
        // transaction the "inner transaction" is treated as part of the "outer" one by simply
        // ignoring the start, commit, and rollback calls in the "inner transaction."
        List<PipelineInstanceNode> pipelineInstanceNodes = performTransaction(() -> {

            List<PipelineInstanceNode> instanceNodes = new ArrayList<>();
            for (PipelineDefinitionNode pipelineDefinitionNode : pipelineDefinitionNodes) {

                // Update the Map from parent to child definition nodes.
                nextNodesByCurrentNode.put(pipelineDefinitionNode,
                    pipelineDefinitionNodeOperations().nextNodes(pipelineDefinitionNode));

                // Get the pipeline module definition and lock it.
                PipelineModuleDefinition moduleDefinition = pipelineModuleDefinitionOperations()
                    .pipelineModuleDefinition(pipelineDefinitionNode.getModuleName());
                pipelineModuleDefinitionOperations().lock(moduleDefinition);

                // Construct the new pipeline instance node and put it into the Map.
                PipelineInstanceNode instanceNode = new PipelineInstanceNode(pipelineDefinitionNode,
                    moduleDefinition);
                instanceNode = pipelineInstanceNodeOperations().merge(instanceNode);
                instanceNodeByPipelineDefinitionNode.put(pipelineDefinitionNode, instanceNode);

                if (parentInstanceNode == null) {
                    pipelineInstanceOperations().addRootNode(pipelineInstance, instanceNode);
                } else {
                    pipelineInstanceOperations().addPipelineInstanceNode(pipelineInstance,
                        instanceNode);
                }

                // Bind the module parameter sets to the instance node.
                pipelineInstanceNodeOperations().bindParameterSets(pipelineDefinitionNode,
                    instanceNode);
                instanceNodes.add(instanceNode);
            }
            if (parentInstanceNode != null) {
                pipelineInstanceNodeOperations().addNextNodes(parentInstanceNode, instanceNodes);
            }
            return instanceNodes;
        });

        // Recursively create instance nodes for the child nodes of each pipeline definition
        // node, unless the node in question is the end node. Note that we need to create these
        // but do not need to return them.
        for (Map.Entry<PipelineDefinitionNode, List<PipelineDefinitionNode>> entry : nextNodesByCurrentNode
            .entrySet()) {
            if (endNode != null && entry.getKey().getId().longValue() == endNode.getId().longValue()
                || entry.getValue().isEmpty()) {
                continue;
            }
            newInstanceNodes(pipelineDefinition, pipelineInstance, entry.getValue(), endNode,
                instanceNodeByPipelineDefinitionNode.get(entry.getKey()));
        }
        return pipelineInstanceNodes;
    }

    /** Creates a new pipeline instance in state PROCESSING with its execution clock started. */
    public PipelineInstance newPipelineInstance(String name, PipelineDefinition pipeline,
        ModelRegistry modelRegistry) {
        return performTransaction(() -> {
            PipelineInstance instance = new PipelineInstance(pipeline);
            instance.setName(name);
            instance.setPipelineDefinition(pipeline);
            pipelineDefinitionOperations().lock(pipeline);
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
     * Creates new {@Link PipelineTask}s for a given {@link PipelineInstanceNode} that are in step
     * WAITING_TO_RUN.
     */
    public List<PipelineTask> newPipelineTasks(PipelineInstanceNode instanceNode,
        PipelineInstance instance, List<UnitOfWork> unitsOfWork) {
        List<PipelineTask> pipelineTasks = new ArrayList<>();
        for (UnitOfWork unitOfWork : unitsOfWork) {
            PipelineTask pipelineTask = new PipelineTask(instance, instanceNode);
            pipelineTask.setProcessingStep(ProcessingStep.WAITING_TO_RUN);
            pipelineTask.setUowTaskParameters(unitOfWork.getParameters());
            pipelineTask = pipelineTaskOperations().merge(pipelineTask);
            pipelineTasks.add(pipelineTask);
        }
        pipelineInstanceNodeOperations().addPipelineTasks(instanceNode, pipelineTasks);
        return pipelineTasks;
    }

    ParametersOperations parametersOperations() {
        return parametersOperations;
    }

    PipelineDefinitionNodeOperations pipelineDefinitionNodeOperations() {
        return pipelineDefinitionNodeOperations;
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

    PipelineDefinitionOperations pipelineDefinitionOperations() {
        return pipelineDefinitionOperations;
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }
}
