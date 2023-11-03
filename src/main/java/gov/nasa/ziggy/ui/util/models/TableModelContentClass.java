package gov.nasa.ziggy.ui.util.models;

/**
 * Interface that provides the ability for table models to report the Java class for their content
 * objects. All Ziggy table models must implement this interface.
 *
 * @author PT
 * @param <T> Class of objects managed by the table model.
 */
public interface TableModelContentClass<T> {

    Class<T> tableModelContentClass();
}
