package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JLabel;

import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.ui.util.HtmlBuilder;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;

/**
 * {@link DisplayModel} for pipeline tasks. This class is used to format pipeline tasks for display
 * on the console.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TasksDisplayModel extends DisplayModel {
    public static final String WORKER = "Worker";
    private static final String[] COLUMN_NAMES = { "ID", "Node", "UOW", WORKER, "Status",
        "Subtasks", "Time" };
    private static final Class<?>[] COLUMN_CLASSES = { String.class, String.class, String.class,
        String.class, String.class, Integer.class, Integer.class };

    public static final int[] COLUMN_WIDTHS = { ZiggySwingUtils.textWidth(new JLabel(), "123456"),
        ZiggySwingUtils.textWidth(new JLabel(), "123456789012345"),
        ZiggySwingUtils.textWidth(new JLabel(), "123456789012345"),
        ZiggySwingUtils.textWidth(new JLabel(), "localhost:99"),
        ZiggySwingUtils.textWidth(new JLabel(), "ERROR - WAITING_TO_STORE"),
        ZiggySwingUtils.textWidth(new JLabel(), "Subtasks"),
        ZiggySwingUtils.textWidth(new JLabel(), "00:00:00") };

    private List<PipelineTaskDisplayData> tasks = new ArrayList<>();
    private TaskCounts taskCounts = new TaskCounts();

    public TasksDisplayModel() {
    }

    public TasksDisplayModel(List<PipelineTaskDisplayData> tasks) {
        update(tasks);
    }

    public TasksDisplayModel(PipelineTaskDisplayData task) {
        update(List.of(task));
    }

    public void update(List<PipelineTaskDisplayData> tasks) {
        this.tasks = tasks;
        taskCounts = new TaskCounts(this.tasks);
    }

    public PipelineTaskDisplayData getPipelineTaskForRow(int row) {
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
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return COLUMN_CLASSES[columnIndex];
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineTaskDisplayData task = tasks.get(rowIndex);

        // Update columnIndex values for subtask modifications below if column index changes.
        Object value = switch (columnIndex) {
            case 0 -> task.getPipelineTaskId();
            case 1 -> task.getPipelineStepName();
            case 2 -> task.getBriefState();
            case 3 -> task.getWorkerName();
            case 4 -> task.getDisplayProcessingStep();
            case 5 -> TaskCounts.subtaskCountsLabel(task.getCompletedSubtaskCount(),
                task.getTotalSubtaskCount(), task.getFailedSubtaskCount());
            case 6 -> task.getExecutionClock();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };

        // Make the whole row red if there's an error.
        if (task.isError()) {
            return HtmlBuilder.htmlBuilder().appendColor(value.toString(), "red").toString();
        }

        return value;
    }

    public TaskCounts getTaskCounts() {
        return taskCounts;
    }
}
