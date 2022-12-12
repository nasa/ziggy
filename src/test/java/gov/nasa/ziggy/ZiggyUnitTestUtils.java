package gov.nasa.ziggy;

import java.util.Collection;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.security.User;

/**
 * General utilities for unit and integration tests.
 *
 * @author Forrest Girouard
 * @author Bill Wohler
 * @author Miles Cote
 */
public class ZiggyUnitTestUtils {

    // The items below perform Hibernate initialization of lazy-loaded elements of a database
    // object. This is necessary because we need to be able to compare the objects with the
    // expected objects, and that comparison necessarily includes lazy-loaded bits.

    // Initialization for database items that are specific to a particular execution of the
    // pipeline:
    // pipeline tasks, instances, instance nodes
    public static void initializePipelineTask(PipelineTask pt) {
        initializePipelineInstance(pt.getPipelineInstance());
        Hibernate.initialize(pt.getPipelineInstanceNode().getModuleParameterSets());
        Hibernate.initialize(pt.getExecLog());
        Hibernate.initialize(pt.getSummaryMetrics());
        Hibernate.initialize(pt.getProducerTaskIds());
        Hibernate.initialize(pt.getRemoteJobs());
    }

    public static void initializePipelineInstance(PipelineInstance pipelineInstance) {
        initializePipelineDefinition(pipelineInstance.getPipelineDefinition());
        Hibernate.initialize(pipelineInstance.getPipelineParameterSets());
    }

    public static void initializePipelineInstanceNode(PipelineInstanceNode node) {
        initializePipelineInstance(node.getPipelineInstance());
        initializePipelineModuleDefinition(node.getPipelineModuleDefinition());
        Hibernate.initialize(node.getModuleParameterSets());
        ZiggyUnitTestUtils.initializePipelineDefinitionNodes(
            node.getPipelineInstance().getPipelineDefinition().getRootNodes());
    }

    // Initialization for database items that define the pipelines: pipeline definitions,
    // pipeline module definitions, pipeline definition nodes
    public static void initializePipelineDefinition(PipelineDefinition pipelineDefinition) {
        initializeUser(pipelineDefinition.getAuditInfo().getLastChangedUser());
        Hibernate.initialize(pipelineDefinition.getRootNodes());
        initializePipelineDefinitionNodes(pipelineDefinition.getRootNodes());
    }

    public static void initializePipelineDefinitionNodes(Collection<PipelineDefinitionNode> nodes) {
        if (nodes != null && !nodes.isEmpty()) {
            for (PipelineDefinitionNode node : nodes) {
                initializePipelineDefinitionNode(node);
            }
        }
    }

    public static void initializePipelineDefinitionNode(PipelineDefinitionNode node) {
        Hibernate.initialize(node.getInputDataFileTypes());
        Hibernate.initialize(node.getOutputDataFileTypes());
        Hibernate.initialize(node.getModelTypes());
        Hibernate.initialize(node.getNextNodes());
        initializePipelineDefinitionNodes(node.getNextNodes());
    }

    public static void initializePipelineModuleDefinition(
        PipelineModuleDefinition moduleDefinition) {
        initializeUser(moduleDefinition.getAuditInfo().getLastChangedUser());
    }

    // Utility initialization of a User instance
    public static void initializeUser(User user) {
        Hibernate.initialize(user.getRoles());
        Hibernate.initialize(user.getPrivileges());
    }

}
