package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.status.StatusPanel.ContentItem.ALERTS;
import static gov.nasa.ziggy.ui.status.StatusPanel.ContentItem.PIPELINES;
import static gov.nasa.ziggy.ui.status.StatusPanel.ContentItem.PROCESSES;
import static gov.nasa.ziggy.ui.status.StatusPanel.ContentItem.WORKERS;

import java.awt.Dimension;

import javax.swing.JLabel;

import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * Displays a set of summary indicators ("idiot lights") for the status of several pipeline systems:
 * pipelines, the worker, processes, alerts, and metrics. The indicators can be green, yellow, or
 * red for various status, or gray if not applicable ("light is out").
 *
 * @author PT
 * @author Bill Wohler
 */
public class StatusSummaryPanel extends javax.swing.JPanel {
    private static final long serialVersionUID = 20230822L;

    public StatusSummaryPanel() {
        buildComponent();
    }

    private void buildComponent() {
        addIdiotLight(PIPELINES.menuItem(), "Pi", "Pipeline instances");
        addIdiotLight(WORKERS.menuItem(), "W", "Workers");
        addIdiotLight(PROCESSES.menuItem(), "Pr", "Processes");
        addIdiotLight(ALERTS.menuItem(), "A", "Alerts");
    }

    private void addIdiotLight(Indicator indicator, String label, String tooltip) {
        JLabel lightLabel = new JLabel(label);
        lightLabel.setToolTipText(tooltip);
        add(lightLabel);
        lightLabel.setMinimumSize(new Dimension(0, 35));
        lightLabel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 35));
        add(indicator.getIdiotLight());
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new StatusSummaryPanel());
    }
}
