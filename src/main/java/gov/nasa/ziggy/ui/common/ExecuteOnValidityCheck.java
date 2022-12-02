package gov.nasa.ziggy.ui.common;

/**
 * Defines an interface that provides a single method, {@link execute}, which in turn takes a
 * boolean argument. The method can thus perform any necessary actions in the event that the
 * argument is true, and any other actions in the event that it is false.
 *
 * @author PT
 */
public interface ExecuteOnValidityCheck {

    void execute(boolean valid);
}
