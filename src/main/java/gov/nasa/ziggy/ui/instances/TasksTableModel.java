package gov.nasa.ziggy.ui.instances;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.SwingWorker;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.PipelineTaskCrudProxy;
import gov.nasa.ziggy.ui.util.proxy.ProcessingSummaryOpsProxy;
import gov.nasa.ziggy.util.TasksStates;
import gov.nasa.ziggy.util.dispmod.TasksDisplayModel;

@SuppressWarnings("serial")
public class TasksTableModel extends AbstractDatabaseModel<PipelineTask> {
    private PipelineInstance pipelineInstance;
    private List<PipelineTask> tasks = new LinkedList<>();
    private Map<Long, ProcessingSummary> taskAttrs = new HashMap<>();
    private final PipelineTaskCrudProxy pipelineTaskCrud;
    private final ProcessingSummaryOpsProxy attrOps;

    // Used to store member values when the instance is TEMPORARILY set to null
    private List<PipelineTask> stashedTasks;
    private Map<Long, ProcessingSummary> stashedAttributes;
    long instanceIdForStashedTaskInfo = -1L;

    private final TasksDisplayModel tasksDisplayModel = new TasksDisplayModel();

    private enum UpdateMode {
        SAME_INSTANCE, REPLACE_TEMP_NULL, FULL_UPDATE
    }

    public TasksTableModel() {
        pipelineTaskCrud = new PipelineTaskCrudProxy();
        attrOps = new ProcessingSummaryOpsProxy();
        stashedTasks = tasks;
        stashedAttributes = taskAttrs;
    }

    @Override
    public void loadFromDatabase() {
        loadFromDatabase(true);
    }

    public void loadFromDatabase(boolean forceUpdate) {
        SwingWorker<Void, Void> swingWorker = new SwingWorker<>() {

            @Override
            protected Void doInBackground() throws Exception {
                try {
                    if (pipelineInstance == null) {
                        tasks = new LinkedList<>();
                        taskAttrs = new HashMap<>();
                    } else {
                        UpdateMode updateMode = getUpdateMode(forceUpdate);
                        switch (updateMode) {
                            case SAME_INSTANCE:
                                break;
                            case REPLACE_TEMP_NULL:
                                tasks = stashedTasks;
                                taskAttrs = stashedAttributes;
                                break;
                            case FULL_UPDATE:
                                tasks = pipelineTaskCrud.retrieveAll(pipelineInstance);
                                taskAttrs = attrOps.retrieveByInstanceId(pipelineInstance.getId());
                                break;
                            default:
                                throw new IllegalStateException(
                                    "Unsupported update mode " + updateMode.toString());
                        }
                    }
                } catch (ConsoleSecurityException ignore) {
                }
                return null;
            }

            @Override
            protected void done() {
                try {
                    tasksDisplayModel.update(tasks, taskAttrs);
                } catch (ConsoleSecurityException ignore) {
                }
                fireTableDataChanged();
            }
        };
        swingWorker.execute();
    }

    /**
     * Determines the update mode to use when refreshing task and task attribute information. This
     * depends on the values of the current instance, the last instance, the stashed instance, and
     * the forceUpdate flag:
     * <ol>
     * <li>If the current instance is the same as the last instance, no update is needed.
     * <li>If the last instance was null, but the current instance matches the stashed instance, we
     * can refresh the task information from the stashed information.
     * <li>If the current instance is different from the last non-null instance (which is either the
     * last instance or the stashed instance), a full update is needed.
     * <li>If the fullUpdate flag is true, then a full update is needed regardless of these other
     * considerations.
     * </ol>
     */
    private UpdateMode getUpdateMode(boolean forceUpdate) {

        UpdateMode updateMode = UpdateMode.FULL_UPDATE;

        if (tasks.isEmpty() && instanceIdForStashedTaskInfo == pipelineInstance.getId()) {
            updateMode = UpdateMode.REPLACE_TEMP_NULL;
        } else if (!tasks.isEmpty() && instanceIdForStashedTaskInfo == pipelineInstance.getId()) {
            updateMode = UpdateMode.SAME_INSTANCE;
        } else if (instanceIdForStashedTaskInfo != pipelineInstance.getId()) {
            updateMode = UpdateMode.FULL_UPDATE;
        }

        if (forceUpdate) {
            updateMode = UpdateMode.FULL_UPDATE;
        }
        return updateMode;
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return tasksDisplayModel.getRowCount();
    }

    @Override
    public int getColumnCount() {
        return tasksDisplayModel.getColumnCount();
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();
        return tasksDisplayModel.getValueAt(rowIndex, columnIndex);
    }

    @Override
    public String getColumnName(int column) {
        return tasksDisplayModel.getColumnName(column);
    }

    public PipelineInstance getPipelineInstance() {
        return pipelineInstance;
    }

    /**
     * Sets the pipeline instance and determines whether it is a genuinely new instance. A genuinely
     * new instance is one that is different from the current instance (if that instance is
     * non-null), or different from the stashed instance (if the current instance is null). This
     * information is used to decide what to do about the selected row in the tasks table.
     */
    public boolean updatePipelineInstance(PipelineInstance pipelineInstance) {
        boolean genuinelyNewInstance = true;
        if (this.pipelineInstance == null
            && pipelineInstance.getId() == instanceIdForStashedTaskInfo) {
            genuinelyNewInstance = false;
        }
        if (this.pipelineInstance != null && this.pipelineInstance.equals(pipelineInstance)) {
            genuinelyNewInstance = false;
        }
        setPipelineInstance(pipelineInstance);
        return genuinelyNewInstance;
    }

    public void setPipelineInstance(PipelineInstance pipelineInstance) {

        // Stash current pipeline instance information. Don't bother to do this if the
        // current pipeline instance is null, as we want to stash the most recent non-null
        // state (i.e., if we get 10 updates to null in a row, we still want the stashed
        // information to be from the non-null update that came before those 10 null updates).
        if (pipelineInstance != null) {
            stashTaskInfo();
        }
        this.pipelineInstance = pipelineInstance;
    }

    public List<Integer> getModelIndicesOfTasks(List<Long> taskIds) {
        List<Integer> modelIndices = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            if (taskIds.contains(tasks.get(i).getId())) {
                modelIndices.add(i);
            }
        }
        return modelIndices;
    }

    /**
     * Stores the current task information state and the current instance ID. This makes it possible
     * to use stashed information to update the object when a new pipeline instance is supplied but
     * the new instance ID is the same as the last non-null instance ID.
     */
    private void stashTaskInfo() {
        stashedTasks = tasks;
        stashedAttributes = taskAttrs;
        instanceIdForStashedTaskInfo = pipelineInstance != null ? pipelineInstance.getId() : -1L;
    }

    public TasksStates getTaskStates() {
        return tasksDisplayModel.getTaskStates();
    }

    @Override
    public PipelineTask getContentAtRow(int row) {
        validityCheck();
        return tasks.get(row);
    }

    @Override
    public Class<PipelineTask> tableModelContentClass() {
        return PipelineTask.class;
    }
}
