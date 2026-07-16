package gov.nasa.ziggy.ui.instances;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JSplitPane;

import gov.nasa.ziggy.services.messages.NoRunningOrQueuedPipelinesMessage;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.MessageUtils;

/**
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class InstancesTasksPanel extends JSplitPane {

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
            instancesPanel = new InstancesPanel(this);
            TasksPanel tasksPanel = new TasksPanel();

            add(instancesPanel, JSplitPane.LEFT);
            add(tasksPanel, JSplitPane.RIGHT);

            setDividerLocation(-1);
            setOneTouchExpandable(true);

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
