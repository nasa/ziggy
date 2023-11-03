package gov.nasa.ziggy.ui.util.models;

import javax.swing.table.AbstractTableModel;

/**
 * Extension of the {@link AbstractTableModel} for Ziggy. This class adds the following features to
 * its superclass:
 * <ol>
 * <li>A method, {@link #getContentAtRow(int)}, that returns the object at a given row in the table.
 * <li>The {@link TableModelContentClass} interface, which returns the class of objects managed by
 * the table model (i.e., the actual value of parameter T).
 * </ol>
 *
 * @author PT
 * @param <T> Class of objects managed by the model.
 */
public abstract class AbstractZiggyTableModel<T> extends AbstractTableModel
    implements TableModelContentClass<T> {

    private static final long serialVersionUID = 20230511L;

    public abstract T getContentAtRow(int row);
}
