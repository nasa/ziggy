package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.util.TasksStates;

/**
 * {@link DisplayModel} for pipeline tasks. This class is used to format pipeline tasks for display
 * on the console.
 *
 * @author Todd Klaus
 */
public class TasksDisplayModel extends DisplayModel {
    private static final int NUM_COLUMNS = 7;

    private List<PipelineTask> tasks = new LinkedList<>();
    private Map<Long, ProcessingSummary> processingStates = new HashMap<>();
    private final TasksStates taskStates = new TasksStates();

    public TasksDisplayModel() {
    }

    public TasksDisplayModel(List<PipelineTask> tasks, Map<Long, ProcessingSummary> taskAttrs) {
        this.tasks = tasks;
        processingStates = taskAttrs;

        taskStates.update(this.tasks, taskAttrs);
    }

    public TasksDisplayModel(PipelineTask task, ProcessingSummary taskAttrs) {
        tasks = new ArrayList<>(100);
        tasks.add(task);
        processingStates = new HashMap<>();
        processingStates.put(task.getId(), taskAttrs);

        taskStates.update(tasks, processingStates);
    }

    public void update(List<PipelineTask> tasks, Map<Long, ProcessingSummary> taskAttrs) {
        this.tasks = tasks;
        processingStates = taskAttrs;

        taskStates.update(this.tasks, taskAttrs);
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
        return NUM_COLUMNS;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        PipelineTask task = tasks.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return task.getId();
            case 1:
                return task.getPipelineInstanceNode().getPipelineModuleDefinition().getName();
            case 2:
                return task.uowTaskInstance().briefState();
            case 3:
                return task.getState();
            case 4:
                return task.getWorkerName();
            case 5:
                return task.elapsedTime();
            case 6:
                ProcessingSummary attributes = processingStates.get(task.getId());
                if (attributes != null) {
                    return attributes.processingStateShortLabel();
                } else {
                    return "???";
                }
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public String getColumnName(int column) {
        return switch (column) {
            case 0 -> "ID";
            case 1 -> "Module";
            case 2 -> "UOW";
            case 3 -> "State";
            case 4 -> "Worker";
            case 5 -> "P-time";
            case 6 -> "P-state";
            default -> throw new IllegalArgumentException("Unexpected value: " + column);
        };
    }

    public TasksStates getTaskStates() {
        return taskStates;
    }
}
