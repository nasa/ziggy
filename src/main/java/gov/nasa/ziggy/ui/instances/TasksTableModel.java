package gov.nasa.ziggy.ui.instances;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.swing.SwingWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDisplayDataOperations;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.ui.util.models.AbstractZiggyTableModel;
import gov.nasa.ziggy.ui.util.models.DatabaseModel;
import gov.nasa.ziggy.ui.util.table.TableUpdater;
import gov.nasa.ziggy.util.dispmod.TasksDisplayModel;

@SuppressWarnings("serial")
public class TasksTableModel extends AbstractZiggyTableModel<PipelineTaskDisplayData>
    implements DatabaseModel {

    private static final Logger log = LoggerFactory.getLogger(TasksTableModel.class);

    /** Preferred column widths. */
    public static final int[] COLUMN_WIDTHS = TasksDisplayModel.COLUMN_WIDTHS;

    private long pipelineInstanceId;
    private PipelineInstance pipelineInstance;
    private List<TaskTimeInfo> tasks = new LinkedList<>();
    private TasksDisplayModel tasksDisplayModel = new TasksDisplayModel();

    private final PipelineTaskOperations pipelineTaskOperations = new PipelineTaskOperations();
    private final PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations = new PipelineTaskDisplayDataOperations();
    private final PipelineInstanceOperations pipelineInstanceOperations = new PipelineInstanceOperations();

    @Override
    public void loadFromDatabase() {
        new SwingWorker<TableUpdater, Void>() {
            @Override
            protected TableUpdater doInBackground() throws Exception {
                List<PipelineTaskDisplayData> pipelineTaskDisplayData;
                if (pipelineInstanceId > 0) {
                    if (pipelineInstance == null
                        || pipelineInstance.getId() != pipelineInstanceId) {
                        pipelineInstance = pipelineInstanceOperations()
                            .pipelineInstance(pipelineInstanceId);
                    }
                    pipelineTaskDisplayData = pipelineTaskDisplayDataOperations()
                        .pipelineTaskDisplayData(pipelineInstance);
                } else {
                    pipelineInstance = null;
                    pipelineTaskDisplayData = new LinkedList<>();
                }
                tasksDisplayModel.update(pipelineTaskDisplayData);
                List<TaskTimeInfo> oldTasks = tasks;
                tasks = pipelineTaskDisplayData.stream()
                    .map(TaskTimeInfo::new)
                    .collect(Collectors.toList());
                return new TableUpdater(oldTasks, tasks);
            }

            @Override
            protected void done() {
                try {
                    get().updateTable(TasksTableModel.this);
                    ZiggyMessenger.publish(new TasksUpdatedMessage(TasksTableModel.this), false);
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Could not load pipeline tasks or attributes", e);
                }
            }
        }.execute();
    }

    @Override
    public int getRowCount() {
        return tasksDisplayModel.getRowCount();
    }

    @Override
    public int getColumnCount() {
        return tasksDisplayModel.getColumnCount();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        return tasksDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    @Override
    public String getColumnName(int column) {
        return tasksDisplayModel.getColumnName(column);
    }

    @Override
    public PipelineTaskDisplayData getContentAtRow(int row) {
        return tasks.get(row).getPipelineTaskDisplayData();
    }

    @Override
    public Class<PipelineTaskDisplayData> tableModelContentClass() {
        return PipelineTaskDisplayData.class;
    }

    public PipelineInstance getPipelineInstance() {
        return pipelineInstance;
    }

    public void updatePipelineInstanceId(long instanceId) {
        if (instanceId == pipelineInstanceId) {
            return;
        }
        pipelineInstanceId = instanceId;
        loadFromDatabase();
    }

    public TaskCounts getTaskStates() {
        return tasksDisplayModel.getTaskCounts();
    }

    PipelineTaskOperations pipelineTaskOperations() {
        return pipelineTaskOperations;
    }

    PipelineTaskDisplayDataOperations pipelineTaskDisplayDataOperations() {
        return pipelineTaskDisplayDataOperations;
    }

    PipelineInstanceOperations pipelineInstanceOperations() {
        return pipelineInstanceOperations;
    }

    private static class TaskTimeInfo {
        private PipelineTaskDisplayData pipelineTaskDisplayData;
        private String time;

        public TaskTimeInfo(PipelineTaskDisplayData pipelineTaskDisplayData) {
            this.pipelineTaskDisplayData = pipelineTaskDisplayData;
            time = getPipelineTaskDisplayData().getExecutionClock().toString();
        }

        public PipelineTaskDisplayData getPipelineTaskDisplayData() {
            return pipelineTaskDisplayData;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineTaskDisplayData, time);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TaskTimeInfo other = (TaskTimeInfo) obj;
            return Objects.equals(pipelineTaskDisplayData, other.pipelineTaskDisplayData)
                && Objects.equals(time, other.time);
        }

        @Override
        public String toString() {
            return "pipelineTask=" + getPipelineTaskDisplayData().toString() + "("
                + getPipelineTaskDisplayData().getProcessingStep() + "), time=" + time;
        }
    }
}
