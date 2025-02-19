package gov.nasa.ziggy;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.hibernate.Hibernate;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;

/**
 * General utilities for unit and integration tests.
 *
 * @author Forrest Girouard
 * @author Bill Wohler
 * @author Miles Cote
 */
public class ZiggyUnitTestUtils {

    /**
     * Location of test/data relative to the working directory (assumed to be the main Ziggy
     * directory, since that's where both Eclipse and Gradle set the working directory when they run
     * tests).
     */
    public static final Path TEST_DATA = Paths.get("test", "data");

    // The items below perform Hibernate initialization of lazy-loaded elements of a database
    // object. This is necessary because we need to be able to compare the objects with the
    // expected objects, and that comparison necessarily includes lazy-loaded bits.

    public static void initializePipelineInstance(PipelineInstance pipelineInstance) {
        Hibernate.initialize(pipelineInstance.getRootNodes());
        Hibernate.initialize(pipelineInstance.getPipelineInstanceNodes());
    }

    public static void initializePipelineInstanceNode(PipelineInstanceNode node) {
        Hibernate.initialize(node.getNextNodes());
        Hibernate.initialize(node.getPipelineTasks());
    }

    // Initialization for database items that define the pipelines: pipeline definitions,
    // pipeline module definitions, pipeline definition nodes
    public static void initializePipelineDefinition(PipelineDefinition pipelineDefinition) {
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
    }
}
