package gov.nasa.ziggy.ui.util;

import java.text.NumberFormat;

import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class FloatingPointTableCellRenderer extends DefaultTableCellRenderer {
    private NumberFormat formatter;

    public FloatingPointTableCellRenderer() {
        setHorizontalAlignment(SwingConstants.RIGHT);
    }

    @Override
    protected void setValue(Object value) {
        if (formatter == null) {
            formatter = NumberFormat.getInstance();
            formatter.setMaximumFractionDigits(15);
        }
        setText(value == null ? "" : formatter.format(value));
    }
}
