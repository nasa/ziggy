package gov.nasa.ziggy.ui.mon.master;

import java.awt.Dimension;
import java.awt.FlowLayout;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.WindowConstants;

/**
 * Displays a set of summary indicators ("idiot lights") for the status of several pipeline systems:
 * pipelines, the worker, processes, alerts, and metrics. The indicators can be green, yellow, or
 * red for various status, or grey if not applicable ("light is out").
 *
 * @author PT
 */
public class StatusSummaryPanel extends javax.swing.JPanel {
    private static final long serialVersionUID = -7538149739596275583L;

    public StatusSummaryPanel() {
        initGUI();
    }

    private void initGUI() {
        try {
            FlowLayout thisLayout = new FlowLayout();
            // setPreferredSize(new Dimension(400, 300));
            thisLayout.setAlignment(FlowLayout.LEFT);
            setLayout(thisLayout);
            addIdiotLight(MasterStatusPanel.instancesIndicator(), "Pi", "Pipeline Instances");
            addIdiotLight(MasterStatusPanel.workersIndicator(), "W", "Worker Threads");
            addIdiotLight(MasterStatusPanel.processesIndicator(), "Pr", "Processes");
            addIdiotLight(MasterStatusPanel.alertsIndicator(), "A", "Alerts");
        } catch (

        Exception e) {
            e.printStackTrace();
        }
    }

    private void addIdiotLight(Indicator indicator, String label, String tooltip) {
        JLabel lightLabel = new JLabel(label);
        lightLabel.setToolTipText(tooltip);
        this.add(lightLabel);
        lightLabel.setMinimumSize(new Dimension(0, 35));
        lightLabel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 35));
        this.add(indicator.getIdiotLight());
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        JFrame frame = new JFrame();
        frame.getContentPane().add(new StatusSummaryPanel());
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
