package gov.nasa.ziggy.ui.instances;

import gov.nasa.ziggy.services.messages.PipelineMessage;

/**
 * Message sent to panels that depend on the task panel's data to indicate that the data in the task
 * panel has been updated.
 *
 * @author Bill Wohler
 */

public class TasksUpdatedMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240917L;

    private final TasksTableModel tasksTableModel;

    public TasksUpdatedMessage(TasksTableModel tasksTableModel) {
        this.tasksTableModel = tasksTableModel;
    }

    public TasksTableModel getTasksTableModel() {
        return tasksTableModel;
    }
}
