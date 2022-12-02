package gov.nasa.ziggy.ui.mon.master;

import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Insets;
import java.awt.LayoutManager;

public class IndicatorLayout implements LayoutManager {
    private int numRows = 3;
    private int hGap = 10;
    private int vGap = 10;

    public IndicatorLayout() {
    }

    @Override
    public void addLayoutComponent(String name, Component comp) {
    }

    @Override
    public void removeLayoutComponent(Component comp) {
    }

    @Override
    public Dimension preferredLayoutSize(Container parent) {
        int width = 0;
        int height = 0;
        int columnHeight = 0;
        int columnWidth = 0;
        int currentRow = 0;

        for (int i = 0; i < parent.getComponentCount(); i++) {
            Component c = parent.getComponent(i);
            Dimension d = c.getPreferredSize();

            columnWidth = (int) Math.max(columnWidth, d.getWidth() + hGap);
            columnHeight += d.getHeight() + vGap;

            if (currentRow + 1 == numRows) {
                // last row
                width += columnWidth;
                height = Math.max(height, columnHeight);
                currentRow = 0;
                columnWidth = 0;
                columnHeight = 0;
            } else {
                currentRow++;
            }
        }

        Insets insets = parent.getInsets();
        return new Dimension(width + insets.left + insets.right,
            height + insets.top + insets.bottom);
    }

    @Override
    public Dimension minimumLayoutSize(Container parent) {
        return preferredLayoutSize(parent);
    }

    @Override
    public void layoutContainer(Container parent) {
        Insets insets = parent.getInsets();
        int x = insets.left + hGap;
        int y = insets.top + vGap;
        int currentRow = 0;
        int columnWidth = 0;

        for (int i = 0; i < parent.getComponentCount(); i++) {
            Component c = parent.getComponent(i);
            Dimension d = c.getPreferredSize();

            c.setBounds(x, y, d.width, d.height);

            columnWidth = (int) Math.max(columnWidth, d.getWidth() + hGap);

            if (currentRow + 1 == numRows) {
                // last row
                x += columnWidth + hGap;
                y = insets.top + vGap;
                currentRow = 0;
                columnWidth = 0;
            } else {
                currentRow++;
                y += d.height + vGap;
            }
        }
    }

    /**
     * @return Returns the hGap.
     */
    public int getHGap() {
        return hGap;
    }

    /**
     * @param gap The hGap to set.
     */
    public void setHGap(int gap) {
        hGap = gap;
    }

    /**
     * @return Returns the numRows.
     */
    public int getNumRows() {
        return numRows;
    }

    /**
     * @param numRows The numRows to set.
     */
    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    /**
     * @return Returns the vGap.
     */
    public int getVGap() {
        return vGap;
    }

    /**
     * @param gap The vGap to set.
     */
    public void setVGap(int gap) {
        vGap = gap;
    }
}
