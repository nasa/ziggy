package gov.nasa.ziggy.uow;

import java.io.Serializable;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.TypedParameterCollection;

/**
 * Defines the unit of work to be processed by a specified task.
 * <p>
 * The unit of work contains a set of parameters, in the form of {@link TypedParameter} instances,
 * that can be used by a pipeline module to determine the range of data to be processed by a given
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
 */
public class UnitOfWork extends TypedParameterCollection
    implements Serializable, Comparable<UnitOfWork> {

    private static final long serialVersionUID = 20230511L;

    public static final String BRIEF_STATE_PARAMETER_NAME = "briefState";

    public String briefState() {
        return getParameter(BRIEF_STATE_PARAMETER_NAME).getString();
    }

    public void setBriefState(String briefState) {
        addParameter(
            new TypedParameter(BRIEF_STATE_PARAMETER_NAME, briefState, ZiggyDataType.ZIGGY_STRING));
    }

    /**
     * Allow the UOWs to sort by brief state.
     */
    @Override
    public int compareTo(UnitOfWork o) {
        return briefState().compareTo(o.briefState());
    }
}
