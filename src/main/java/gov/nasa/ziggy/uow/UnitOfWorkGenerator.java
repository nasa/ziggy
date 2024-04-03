package gov.nasa.ziggy.uow;

import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;

/**
 * Interface for all unit of work generators. A unit of work generator constructs instances of the
 * {@link UnitOfWork} class that can be used by pipeline modules to determine which set of data the
 * corresponding pipeline task should process (for example: a time range).
 * <p>
 * Implementations of {@link UnitOfWorkGenerator} are required to provide the
 * {@link #generateUnitsOfWork(PipelineInstanceNode)}, method, which generates the units of work for
 * a given {@link PipelineInstanceNode}. They may also optionally override the
 * {@link #generateUnitsOfWork(PipelineInstanceNode, Set)} method, which allows a UOW generator to
 * make use of event labels from a Ziggy event generator.
 *
 * @author Todd Klaus
 * @author PT
 */
public interface UnitOfWorkGenerator {

    String GENERATOR_CLASS_PARAMETER_NAME = "uowGenerator";

    /** Constructs completely-populated units of work, including brief state. */
    default List<UnitOfWork> unitsOfWork(PipelineInstanceNode pipelineInstanceNode) {
        List<UnitOfWork> unitsOfWork = generateUnitsOfWork(pipelineInstanceNode);
        setBriefStates(unitsOfWork, pipelineInstanceNode);
        return unitsOfWork;
    }

    /**
     * Constructs completely populated units of work, including brief state and making use of event
     * labels.
     */
    default List<UnitOfWork> unitsOfWork(PipelineInstanceNode pipelineInstanceNode,
        Set<String> eventLabels) {
        List<UnitOfWork> unitsOfWork = generateUnitsOfWork(pipelineInstanceNode, eventLabels);
        setBriefStates(unitsOfWork, pipelineInstanceNode);
        return unitsOfWork;
    }

    /** Assigns brief states to a {@Link List} of {@link UnitOfWork} instances. */
    default void setBriefStates(List<UnitOfWork> unitsOfWork,
        PipelineInstanceNode pipelineInstanceNode) {
        for (UnitOfWork uow : unitsOfWork) {
            setBriefState(uow, pipelineInstanceNode);
        }
    }

    /**
     * Generate the units of work for the current module in the current pipeline run.
     */
    List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode);

    /**
     * Generate units of work, taking into account labels from an event. By default, the event
     * labels are ignored. Override this method to make a UOW generator capable of using the event
     * labels.
     */
    default List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode,
        Set<String> eventLabels) {
        return generateUnitsOfWork(pipelineInstanceNode);
    }

    /**
     * Determines the brief state string for a given {@link UnitOfWork} instance. This is used by
     * the {@link #unitsOfWork(PipelineInstanceNode)} methods to ensure that the brief state is set
     * before returning the UOWs to a caller.
     *
     * @param uow
     * @param pipelineInstanceNode
     */
    void setBriefState(UnitOfWork uow, PipelineInstanceNode pipelineInstanceNode);
}
