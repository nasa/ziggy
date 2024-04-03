package gov.nasa.ziggy.util.dispmod;

/**
 * Interface that provides the ability for display models to report the Java class for their content
 * objects. All Ziggy table models must implement this interface.
 *
 * @author PT
 * @param <T> Class of objects managed by the table model.
 */
public interface ModelContentClass<T> {

    Class<T> tableModelContentClass();
}
