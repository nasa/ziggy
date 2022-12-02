package gov.nasa.ziggy.util.dispmod;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.util.TaskProcessingTimeStats;

/**
 * Aggregates and displays stats for processing times for the {@link PipelineTask}s that make up the
 * specified {@link PipelineInstance}.
 * <p>
 * Sum, max, min, mean, and standard deviation are provided for each module/state combination.
 *
 * @author Todd Klaus
 */
public class PipelineStatsDisplayModel extends DisplayModel {
    private List<ProcessingStatistics> stats = new LinkedList<>();

    public PipelineStatsDisplayModel(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        update(tasks, orderedModuleNames);
    }

    private void update(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        stats = new LinkedList<>();

        Map<String, Map<State, List<PipelineTask>>> moduleStats = new HashMap<>();

        for (PipelineTask task : tasks) {
            String moduleName = task.getPipelineInstanceNode()
                .getPipelineModuleDefinition()
                .toString();

            Map<State, List<PipelineTask>> moduleMap = moduleStats.get(moduleName);
            if (moduleMap == null) {
                moduleMap = new HashMap<>();
                moduleStats.put(moduleName, moduleMap);
            }

            State state = task.getState();
            List<PipelineTask> tasksSubList = moduleMap.get(state);
            if (tasksSubList == null) {
                tasksSubList = new LinkedList<>();
                moduleMap.put(state, tasksSubList);
            }

            tasksSubList.add(task);
        }

        State[] states = State.values();

        for (String moduleName : orderedModuleNames) {
            Map<State, List<PipelineTask>> moduleMap = moduleStats.get(moduleName);

            for (State state : states) {
                if (state != State.SUBMITTED) {
                    List<PipelineTask> tasksSubList = moduleMap.get(state);
                    if (tasksSubList != null) {
                        TaskProcessingTimeStats s = TaskProcessingTimeStats.of(tasksSubList);

                        stats.add(new ProcessingStatistics(moduleName, state, s));
                    }
                }
            }
        }
    }

    @Override
    public int getRowCount() {
        return stats.size();
    }

    @Override
    public int getColumnCount() {
        return 11;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        ProcessingStatistics statsForTaskType = stats.get(rowIndex);
        TaskProcessingTimeStats s = statsForTaskType.getProcessingStats();

        switch (columnIndex) {
            case 0:
                return statsForTaskType.getModuleName();
            case 1:
                return statsForTaskType.getState();
            case 2:
                return s.getCount();
            case 3:
                return formatDouble(s.getSum());
            case 4:
                return formatDouble(s.getMin());
            case 5:
                return formatDouble(s.getMax());
            case 6:
                return formatDouble(s.getMean());
            case 7:
                return formatDouble(s.getStddev());
            case 8:
                return formatDate(s.getMinStart());
            case 9:
                return formatDate(s.getMaxEnd());
            case 10:
                return formatDouble(s.getTotalElapsed());
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Module";
            case 1:
                return "State";
            case 2:
                return "Count";
            case 3:
                return "Sum (hrs)";
            case 4:
                return "Min (hrs)";
            case 5:
                return "Max (hrs)";
            case 6:
                return "Mean (hrs)";
            case 7:
                return "Std (hrs)";
            case 8:
                return "Start";
            case 9:
                return "End";
            case 10:
                return "Elapsed (hrs)";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }

    private static class ProcessingStatistics {

        private final String moduleName;
        private final State state;
        private final TaskProcessingTimeStats processingStats;

        public ProcessingStatistics(String moduleName, State state,
            TaskProcessingTimeStats processingStats) {
            this.moduleName = moduleName;
            this.state = state;
            this.processingStats = processingStats;
        }

        public String getModuleName() {
            return moduleName;
        }

        public State getState() {
            return state;
        }

        public TaskProcessingTimeStats getProcessingStats() {
            return processingStats;
        }

        @Override
        public int hashCode() {
            return Objects.hash(moduleName, processingStats, state);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ProcessingStatistics other = (ProcessingStatistics) obj;
            return Objects.equals(moduleName, other.moduleName)
                && Objects.equals(processingStats, other.processingStats) && state == other.state;
        }
    }
}
