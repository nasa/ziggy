package gov.nasa.ziggy.ui.status;

import static gov.nasa.ziggy.ui.instances.InstancesTasksPanelAutoRefresh.REFRESH_INTERVAL_MILLIS;

import java.awt.BorderLayout;
import java.awt.event.HierarchyEvent;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.JComponent;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceFilter;
import gov.nasa.ziggy.ui.instances.InstancesTable;

/**
 * Contains a list of recent pipelines that have errored out.
 *
 * @author Bill Wohler
 */
public class PipelinesStatusPanel extends javax.swing.JPanel implements Runnable {
    private static final long serialVersionUID = 20230822L;

    private InstancesTable instancesTable;
    private JScrollBar verticalScrollBar;
    private ScheduledFuture<?> scheduledFuture;
    private boolean atBottom;

    public PipelinesStatusPanel() {
        buildComponent();
        addHierarchyListener(this::hierarchyListener);
    }

    // Only run the time when this panel is visible.
    // TODO Replace the timer with a PipelineStateChangedMessage
    private void hierarchyListener(HierarchyEvent evt) {
        JComponent component = (JComponent) evt.getSource();
        if ((evt.getChangeFlags() & HierarchyEvent.SHOWING_CHANGED) != 0) {
            if (component.isShowing()) {
                scheduledFuture = new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(this,
                    0L, REFRESH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            } else if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduledFuture = null;
            }
        }
    }

    private void buildComponent() {
        setLayout(new BorderLayout());

        // Display all instances. Can't scroll to the bottom until the dialog has been packed.
        instancesTable = new InstancesTable(new PipelineInstanceFilter(null, null, 0));
        instancesTable.loadFromDatabase();
        JScrollPane instancesTableScrollPane = new JScrollPane(instancesTable.getTable());
        verticalScrollBar = instancesTableScrollPane.getVerticalScrollBar();
        add(instancesTableScrollPane, BorderLayout.CENTER);
    }

    public void scrollToBottom() {
        // Table has not yet been rendered.
        if (instancesTable.getTable().getWidth() < 10) {
            return;
        }
        SwingUtilities.invokeLater(() -> {
            verticalScrollBar.setValue(verticalScrollBar.getMaximum());
        });
        atBottom = true;
    }

    @Override
    public void run() {
        instancesTable.loadFromDatabase();
        if (!atBottom) {
            scrollToBottom();
        }
    }
}
