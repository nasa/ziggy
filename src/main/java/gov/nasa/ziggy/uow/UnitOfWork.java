package gov.nasa.ziggy.uow;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinTable;

/**
 * Defines the unit of work to be processed by a specified task.
 * <p>
 * The unit of work contains a set of parameters, in the form of {@link Parameter} instances, that
 * can be used by a pipeline module to determine the range of data to be processed by a given
 * pipeline task. For example, for units of work based on time range, the parameters might be the
 * start time and stop time for the task.
 * <p>
 * The precise set of parameters is defined by the {@link UnitOfWorkGenerator} that is invoked to
 * generate the {@link UnitOfWork} instances for a particular node in a particular pipeline. Thus,
 * in the example above, the unit of work generator would produce one or more instances of the
 * {@link UnitOfWork} that have parameters indicating the start and stop times for each task.
 * <p>
 * All instances of {@link UnitOfWork} must have a {@link String} property, "briefState," which is
 * used to identify the UOW of each pipeline task when displayed on the pipeline console. The UOW
 * generators must populate this property. The {@link #setBriefState(String)} method allows the
 * brief state value to be set for the unit of work.
 *
 * @author PT
 * @author Bill Wohler
 */
@Embeddable
public class UnitOfWork implements Serializable, Comparable<UnitOfWork> {

    private static final long serialVersionUID = 20240819L;

    public static final String BRIEF_STATE_PARAMETER_NAME = "briefState";

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_UnitOfWork_parameters")
    private Set<Parameter> parameters = new HashSet<>();

    public UnitOfWork() {
    }

    public UnitOfWork(String briefState) {
        setBriefState(briefState);
    }

    public String briefState() {
        return getParameter(BRIEF_STATE_PARAMETER_NAME).getString();
    }

    public void setBriefState(String briefState) {
        checkNotNull(briefState, "briefState");
        checkArgument(!StringUtils.isBlank(briefState), "briefState can't be empty");
        addParameter(
            new Parameter(BRIEF_STATE_PARAMETER_NAME, briefState, ZiggyDataType.ZIGGY_STRING));
    }

    public void addParameter(Parameter parameter) {
        checkNotNull(parameter, "parameter");
        if (!parameters.add(parameter)) {
            // A parameter with the same name already exists.
            parameters.remove(parameter);
            parameters.add(parameter);
        }
    }

    public Parameter getParameter(String name) {
        for (Parameter parameter : parameters) {
            if (parameter.getName().equals(name)) {
                return parameter;
            }
        }
        return null;
    }

    public Set<Parameter> getParameters() {
        return new HashSet<>(parameters);
    }

    public void setParameters(Collection<Parameter> parameters) {
        checkNotNull(parameters, "parameters");
        checkArgument(!CollectionUtils.isEmpty(parameters), "parameters can't be empty");
        this.parameters.clear();
        for (Parameter parameter : parameters) {
            addParameter(parameter);
        }
    }

    /**
     * Allow the UOWs to sort by brief state.
     */
    @Override
    public int compareTo(UnitOfWork o) {
        return briefState().compareTo(o.briefState());
    }
}
