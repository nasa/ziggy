package gov.nasa.ziggy.ui.common;

import java.awt.FontMetrics;
import java.awt.LayoutManager;
import java.awt.Rectangle;
import java.awt.event.MouseEvent;

import javax.swing.JPanel;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;

public class ToolTipBorderJPanel extends JPanel {

    private static final long serialVersionUID = 20210812L;

    public ToolTipBorderJPanel() {
    }

    public ToolTipBorderJPanel(boolean isDoubleBuffered) {
        super(isDoubleBuffered);
    }

    public ToolTipBorderJPanel(LayoutManager layout) {
        super(layout);
    }

    public ToolTipBorderJPanel(LayoutManager layout, boolean isDoubleBuffered) {
        super(layout, isDoubleBuffered);
    }

    @Override
    public String getToolTipText(MouseEvent e) {
        Border border = getBorder();

        if (border instanceof TitledBorder) {
            TitledBorder tb = (TitledBorder) border;
            FontMetrics fm = getFontMetrics(tb.getTitleFont());
            int titleWidth = fm.stringWidth(tb.getTitle()) + 20;
            Rectangle bounds = new Rectangle(0, 0, titleWidth, fm.getHeight());
            return bounds.contains(e.getPoint()) ? super.getToolTipText() : null;
        }

        return super.getToolTipText(e);
    }

}
