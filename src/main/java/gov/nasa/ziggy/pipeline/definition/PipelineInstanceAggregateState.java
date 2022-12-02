package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;

/**
 * Holds the results of an aggregate query from
 * {@link PipelineInstanceCrud#instanceState(PipelineInstance)}.
 *
 * @author Todd Klaus
 */
public class PipelineInstanceAggregateState {
    private Long numTasks;
    private Long numSubmittedTasks;
    private Long numCompletedTasks;
    private Long numFailedTasks;

    public PipelineInstanceAggregateState(Long numTasks, Long numSubmittedTasks,
        Long numCompletedTasks, Long numFailedTasks) {
        this.numTasks = numTasks;
        this.numSubmittedTasks = numSubmittedTasks;
        this.numCompletedTasks = numCompletedTasks;
        this.numFailedTasks = numFailedTasks;
    }

    public Long getNumTasks() {
        return numTasks;
    }

    public void setNumTasks(Long numTasks) {
        this.numTasks = numTasks;
    }

    public Long getNumCompletedTasks() {
        return numCompletedTasks;
    }

    public void setNumCompletedTasks(Long numCompletedTasks) {
        this.numCompletedTasks = numCompletedTasks;
    }

    public Long getNumFailedTasks() {
        return numFailedTasks;
    }

    public void setNumFailedTasks(Long numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    public Long getNumSubmittedTasks() {
        return numSubmittedTasks;
    }

    public void setNumSubmittedTasks(Long numSubmittedTasks) {
        this.numSubmittedTasks = numSubmittedTasks;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numCompletedTasks, numFailedTasks, numSubmittedTasks, numTasks);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PipelineInstanceAggregateState other = (PipelineInstanceAggregateState) obj;
        if (!Objects.equals(numCompletedTasks, other.numCompletedTasks)) {
            return false;
        }
        if (!Objects.equals(numFailedTasks, other.numFailedTasks)) {
            return false;
        }
        if (!Objects.equals(numSubmittedTasks, other.numSubmittedTasks)) {
            return false;
        }
        if (!Objects.equals(numTasks, other.numTasks)) {
            return false;
        }
        return true;
    }
}
