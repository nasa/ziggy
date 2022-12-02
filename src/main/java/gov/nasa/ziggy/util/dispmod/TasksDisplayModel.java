package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger(TasksDisplayModel.class);
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
                return task.getPipelineInstanceNode().getPipelineModuleDefinition().toString();
            case 2:
                try {
                    return task.uowTaskInstance().briefState();
                } catch (Exception e) {
                    log.error("Exception while getting unit of work", e);
                    return "Excepton.";
                }
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
        switch (column) {
            case 0:
                return "ID";
            case 1:
                return "Module";
            case 2:
                return "UOW";
            case 3:
                return "State";
            case 4:
                return "Worker";
            case 5:
                return "P-time";
            case 6:
                return "P-state";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }

    public TasksStates getTaskStates() {
        return taskStates;
    }
}
