package gov.nasa.ziggy.ui.instances;

import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.GroupLayout;
import javax.swing.LayoutStyle.ComponentPlacement;

import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;

/**
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstancesTasksPanel extends javax.swing.JPanel {

    private static final int BORDER_WIDTH = 10;

    private InstancesPanel instancesPanel;
    private InstancesTasksPanelAutoRefresh instancesTasksPanelAutoRefresh;

    // Indices in the tasks table of the selected tasks. Not to be confused with the
    // task IDs of the selected tasks (see below).
    protected List<Integer> selectedTasksIndices = new ArrayList<>();

    public InstancesTasksPanel() {

        instancesTasksPanelAutoRefresh = buildComponent();

        ZiggyMessenger.subscribe(NoRunningOrQueuedPipelinesMessage.class,
            this::clearInstancesRemaining);
    }

    private InstancesTasksPanelAutoRefresh buildComponent() {
        try {
            setBorder(BorderFactory.createEmptyBorder(BORDER_WIDTH, BORDER_WIDTH, 0, 0));

            instancesPanel = new InstancesPanel(this);
            TasksPanel tasksPanel = new TasksPanel();

            GroupLayout layout = new GroupLayout(this);
            setLayout(layout);

            layout.setHorizontalGroup(layout.createSequentialGroup()
                .addComponent(instancesPanel)
                .addPreferredGap(ComponentPlacement.RELATED)
                .addComponent(tasksPanel));

            layout.setVerticalGroup(
                layout.createParallelGroup().addComponent(instancesPanel).addComponent(tasksPanel));

            InstancesTasksPanelAutoRefresh instancesTasksPanelAutoRefresh = new InstancesTasksPanelAutoRefresh(
                instancesPanel.instancesTable(), tasksPanel.tasksTableModel());
            instancesTasksPanelAutoRefresh.start();
            return instancesTasksPanelAutoRefresh;
        } catch (Exception e) {
            MessageUtils.showError(this, e);
            return null;
        }
    }

    private void clearInstancesRemaining(NoRunningOrQueuedPipelinesMessage message) {
        instancesTasksPanelAutoRefresh.clearInstancesRemaining();
    }
}
