package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JLabel;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * {@link DisplayModel} for pipeline tasks. This class is used to format pipeline tasks for display
 * on the console.
 *
 * @author Todd Klaus
 */
public class TasksDisplayModel extends DisplayModel {
    private static final String[] COLUMN_NAMES = { "ID", "Module", "UOW", "Worker", "Status",
        "Subtasks", "Time" };
    public static final int[] COLUMN_WIDTHS = { ZiggySwingUtils.textWidth(new JLabel(), "123456"),
        ZiggySwingUtils.textWidth(new JLabel(), "123456789012345"),
        ZiggySwingUtils.textWidth(new JLabel(), "123456789012345"),
        ZiggySwingUtils.textWidth(new JLabel(), "localhost:99"),
        ZiggySwingUtils.textWidth(new JLabel(), "ERROR - WAITING_TO_STORE"),
        ZiggySwingUtils.textWidth(new JLabel(), "Subtasks"),
        ZiggySwingUtils.textWidth(new JLabel(), "00:00:00") };

    private List<PipelineTask> tasks = new LinkedList<>();
    private final TaskCounts taskCounts;

    public TasksDisplayModel() {
        taskCounts = new TaskCounts();
    }

    public TasksDisplayModel(List<PipelineTask> tasks) {
        this.tasks = tasks;

        taskCounts = new TaskCounts(this.tasks);
    }

    public TasksDisplayModel(PipelineTask task) {
        tasks = new ArrayList<>(100);
        tasks.add(task);
        taskCounts = new TaskCounts(tasks);
    }

    public PipelineTask getPipelineTaskForRow(int row) {
        return tasks.get(row);
    }

    @Override
    public int getRowCount() {
        return tasks.size();
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineTask task = tasks.get(rowIndex);

        String value = switch (columnIndex) {
            case 0 -> task.getId().toString();
            case 1 -> task.getModuleName();
            case 2 -> task.uowTaskInstance().briefState();
            case 3 -> task.getWorkerName();
            case 4 -> task.getDisplayProcessingStep();
            case 5 -> TaskCounts.subtaskCountsLabel(task.getCompletedSubtaskCount(),
                task.getTotalSubtaskCount(), task.getFailedSubtaskCount());
            case 6 -> task.elapsedTime();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };

        // Make the whole row red if there's an error.
        if (task.isError()) {
            value = HtmlBuilder.htmlBuilder().appendColor(value, "red").toString();
        }

        return value;
    }

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    public TaskCounts getTaskCounts() {
        return taskCounts;
    }
}
