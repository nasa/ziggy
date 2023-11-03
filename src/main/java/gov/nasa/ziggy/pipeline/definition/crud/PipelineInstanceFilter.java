package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;

/**
 * Filter used by the {@link PipelineInstanceCrud#retrieve(PipelineInstanceFilter)} method.
 *
 * @author Todd Klaus
 */
public class PipelineInstanceFilter {

    public static final int DEFAULT_AGE = 10;

    /**
     * Pass if PipelineInstance.name contains the specified String. If empty or null, name is not
     * included in the where clause.
     */
    private String nameContains = "";

    /**
     * Pass if PipelineInstance.state is contained in this Set. If null, state is not included in
     * the where clause.
     */
    private Set<PipelineInstance.State> states;

    /**
     * Pass if PipelineInstance.startProcessingTime is within ageDays days of the time the query is
     * ran. If 0, startProcessingTime is not included in the where clause.
     */
    private int ageDays = DEFAULT_AGE;

    public PipelineInstanceFilter() {
    }

    public PipelineInstanceFilter(String nameContains, Set<State> states, int ageDays) {
        this.nameContains = nameContains;
        this.states = states;
        this.ageDays = ageDays;
    }

    public String getNameContains() {
        return nameContains;
    }

    public void setNameContains(String nameContains) {
        this.nameContains = nameContains;
    }

    public Set<PipelineInstance.State> getStates() {
        return states;
    }

    public void setStates(Set<PipelineInstance.State> states) {
        this.states = states;
    }

    public int getAgeDays() {
        return ageDays;
    }

    public void setAgeDays(int ageDays) {
        this.ageDays = ageDays;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
