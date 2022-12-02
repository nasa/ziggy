package gov.nasa.ziggy.ui.mon.master;

import java.awt.BorderLayout;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 * Superclass for all indicator panels.
 * <p>
 * An indicator panel is a {@link JPanel} that displays {@link Indicator} objects using the custom
 * {@link IndicatorLayout}. These Indicator objects display the realtime state of some system
 * element (a process, a worker thread, a pipeline instance, a metric, etc.) with a color-coded
 * health bar (green, amber, red, gray).
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public abstract class IndicatorPanel extends StatusPanel {
    private int numRows = 5;
    private IndicatorLayout layout;
    private JPanel dataPanel;
    private JScrollPane scrollPane;
    private JButton resetButton;
    private JLabel titleLabel;
    private JPanel titleButtonBarPanel;

    private boolean hasTitleButtonBar = true;

    protected Indicator parentIndicator;
    private JPanel buttonBarPanel;
    private JPanel titlePanel;

    public IndicatorPanel(Indicator parentIndicator) {
        this.parentIndicator = parentIndicator;
        initGUI();
    }

    public IndicatorPanel(Indicator parentIndicator, int numRows, boolean hasTitleButtonBar) {
        this.parentIndicator = parentIndicator;
        this.numRows = numRows;
        this.hasTitleButtonBar = hasTitleButtonBar;
        initGUI();
    }

    public abstract void dismissAll();

    public void removeIndicator(Indicator indicator) {
        dataPanel.remove(indicator);
        repaint();
    }

    private void initGUI() {
        try {
            BorderLayout thisLayout = new BorderLayout();
            setLayout(thisLayout);
            scrollPane = getScrollPane();
            if (hasTitleButtonBar) {
                this.add(getTitleButtonBarPanel(), BorderLayout.NORTH);
            }
            add(scrollPane, BorderLayout.CENTER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JScrollPane getScrollPane() {
        if (scrollPane == null) {
            scrollPane = new JScrollPane();
            scrollPane.setViewportView(getDataPanel());
        }
        return scrollPane;
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();

            layout = new IndicatorLayout();
            layout.setNumRows(numRows);
            dataPanel.setLayout(layout);

            // dataPanel.setBackground(new java.awt.Color(255, 255, 255));
            // dataPanel.setPreferredSize(new Dimension(1500, 1500));
        }
        return dataPanel;
    }

    public void setTitle(String title) {
        titleLabel.setText(title);
    }

    public void add(Indicator indicator) {
        dataPanel.add(indicator);
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

    private JPanel getTitleButtonBarPanel() {
        if (titleButtonBarPanel == null) {
            titleButtonBarPanel = new JPanel();
            BorderLayout titleButtonBarPanelLayout = new BorderLayout();
            titleButtonBarPanel.setLayout(titleButtonBarPanelLayout);
            titleButtonBarPanel.add(getTitlePanel(), BorderLayout.WEST);
            titleButtonBarPanel.add(getButtonBarPanel(), BorderLayout.EAST);
        }
        return titleButtonBarPanel;
    }

    private JLabel getTitleLabel() {
        if (titleLabel == null) {
            titleLabel = new JLabel();
            titleLabel.setText("(title)");
            titleLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }
        return titleLabel;
    }

    private JButton getResetButton() {
        if (resetButton == null) {
            resetButton = new JButton();
            resetButton.setText("reset");
            resetButton.addActionListener(evt -> resetButtonActionPerformed());
        }
        return resetButton;
    }

    private void resetButtonActionPerformed() {
        dismissAll();
    }

    private JPanel getTitlePanel() {
        if (titlePanel == null) {
            titlePanel = new JPanel();
            FlowLayout titlePanelLayout = new FlowLayout();
            titlePanelLayout.setAlignment(FlowLayout.LEFT);
            titlePanel.setLayout(titlePanelLayout);
            titlePanel.add(getTitleLabel());
        }
        return titlePanel;
    }

    private JPanel getButtonBarPanel() {
        if (buttonBarPanel == null) {
            buttonBarPanel = new JPanel();
            FlowLayout buttonBarPanelLayout = new FlowLayout();
            buttonBarPanelLayout.setAlignOnBaseline(true);
            buttonBarPanelLayout.setAlignment(FlowLayout.RIGHT);
            buttonBarPanel.setLayout(buttonBarPanelLayout);
            buttonBarPanel.add(getResetButton());
        }
        return buttonBarPanel;
    }
}
