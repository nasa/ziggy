package gov.nasa.ziggy.util.dispmod;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.util.TaskProcessingTimeStats;
import gov.nasa.ziggy.util.dispmod.PipelineStatsDisplayModel.ProcessingStatistics;

/**
 * Aggregates and displays stats for processing times for the {@link PipelineTask}s that make up the
 * specified {@link PipelineInstance}.
 * <p>
 * Sum, max, min, mean, and standard deviation are provided for each module/processingStep
 * combination.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class PipelineStatsDisplayModel extends DisplayModel
    implements ModelContentClass<ProcessingStatistics> {

    private static final String[] COLUMN_NAMES = { "Module", "Status", "Count", "Sum (hrs)",
        "Min (hrs)", "Max (hrs)", "Mean (hrs)", "Std (hrs)", "Start", "End", "Elapsed (hrs)" };

    private static final String ERROR = "ERROR";

    private List<ProcessingStatistics> stats = new LinkedList<>();

    public PipelineStatsDisplayModel(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        update(tasks, orderedModuleNames);
    }

    private void update(List<PipelineTask> tasks, List<String> orderedModuleNames) {
        stats = new LinkedList<>();

        Map<String, Map<String, List<PipelineTask>>> moduleStats = new HashMap<>();

        for (PipelineTask task : tasks) {
            String moduleName = task.getModuleName();

            Map<String, List<PipelineTask>> moduleMap = moduleStats.get(moduleName);
            if (moduleMap == null) {
                moduleMap = new HashMap<>();
                moduleStats.put(moduleName, moduleMap);
            }

            List<PipelineTask> tasksSubList = moduleMap.get(displayProcessingStep(task));
            if (tasksSubList == null) {
                tasksSubList = new LinkedList<>();
                moduleMap.put(displayProcessingStep(task), tasksSubList);
            }

            tasksSubList.add(task);
        }

        for (String moduleName : orderedModuleNames) {
            Map<String, List<PipelineTask>> moduleMap = moduleStats.get(moduleName);

            updateStats(moduleName, moduleMap, ERROR);
            for (ProcessingStep processingStep : ProcessingStep.values()) {
                updateStats(moduleName, moduleMap, processingStep.toString());
            }
        }
    }

    private void updateStats(String moduleName, Map<String, List<PipelineTask>> moduleMap,
        String displayProcessingStep) {
        List<PipelineTask> tasksSubList = moduleMap.get(displayProcessingStep);
        if (tasksSubList != null) {
            TaskProcessingTimeStats s = TaskProcessingTimeStats.of(tasksSubList);
            stats.add(new ProcessingStatistics(moduleName, displayProcessingStep, s));
        }
    }

    private String displayProcessingStep(PipelineTask task) {
        return task.isError() ? ERROR : task.getProcessingStep().toString();
    }

    @Override
    public int getRowCount() {
        return stats.size();
    }

    @Override
    public int getColumnCount() {
        return COLUMN_NAMES.length;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        ProcessingStatistics statsForTaskType = stats.get(rowIndex);
        TaskProcessingTimeStats s = statsForTaskType.getProcessingStats();

        return switch (columnIndex) {
            case 0 -> statsForTaskType.getModuleName();
            case 1 -> statsForTaskType.getDisplayProcessingStep();
            case 2 -> s.getCount();
            case 3 -> formatDouble(s.getSum());
            case 4 -> formatDouble(s.getMin());
            case 5 -> formatDouble(s.getMax());
            case 6 -> formatDouble(s.getMean());
            case 7 -> formatDouble(s.getStddev());
            case 8 -> formatDate(s.getMinStart());
            case 9 -> formatDate(s.getMaxEnd());
            case 10 -> formatDouble(s.getTotalElapsed());
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    @Override
    public String getColumnName(int column) {
        return COLUMN_NAMES[column];
    }

    @Override
    public Class<ProcessingStatistics> tableModelContentClass() {
        return ProcessingStatistics.class;
    }

    public static class ProcessingStatistics {

        private final String moduleName;
        private final String displayProcessingStep;
        private final TaskProcessingTimeStats processingStats;

        public ProcessingStatistics(String moduleName, String displayProcessingStep,
            TaskProcessingTimeStats processingStats) {
            this.moduleName = moduleName;
            this.displayProcessingStep = displayProcessingStep;
            this.processingStats = processingStats;
        }

        public String getModuleName() {
            return moduleName;
        }

        public String getDisplayProcessingStep() {
            return displayProcessingStep;
        }

        public TaskProcessingTimeStats getProcessingStats() {
            return processingStats;
        }

        @Override
        public int hashCode() {
            return Objects.hash(moduleName, processingStats, displayProcessingStep);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ProcessingStatistics other = (ProcessingStatistics) obj;
            return Objects.equals(moduleName, other.moduleName)
                && Objects.equals(processingStats, other.processingStats)
                && displayProcessingStep == other.displayProcessingStep;
        }
    }
}
