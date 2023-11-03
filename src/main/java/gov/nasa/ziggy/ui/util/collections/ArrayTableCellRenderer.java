package gov.nasa.ziggy.ui.util.collections;

import com.l2fprod.common.swing.renderer.DefaultCellRenderer;

import gov.nasa.ziggy.collections.ZiggyDataType;

/**
 * Render arrays as comma-separated Strings
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class ArrayTableCellRenderer extends DefaultCellRenderer {
    @Override
    public void setValue(Object value) {
        setText(value != null ? ZiggyDataType.objectToString(value) : "");
    }
}
