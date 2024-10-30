package gov.nasa.ziggy.util.dispmod;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskDisplayData;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
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

    private static final String[] COLUMN_NAMES = { "Module", "Start", "Status", "Count",
        "Sum (hrs)", "Min (hrs)", "Max (hrs)", "Mean (hrs)", "Std (hrs)" };

    private static final String ERROR = "ERROR";

    private List<ProcessingStatistics> stats = new LinkedList<>();

    public PipelineStatsDisplayModel(List<PipelineTaskDisplayData> tasks,
        List<String> orderedModuleNames) {
        update(tasks, orderedModuleNames);
    }

    private void update(List<PipelineTaskDisplayData> tasks, List<String> orderedModuleNames) {
        stats = new LinkedList<>();

        Map<String, Map<String, List<PipelineTaskDisplayData>>> moduleStats = new HashMap<>();

        for (PipelineTaskDisplayData task : tasks) {
            String moduleName = task.getModuleName();

            Map<String, List<PipelineTaskDisplayData>> moduleMap = moduleStats.get(moduleName);
            if (moduleMap == null) {
                moduleMap = new HashMap<>();
                moduleStats.put(moduleName, moduleMap);
            }

            List<PipelineTaskDisplayData> tasksSubList = moduleMap.get(displayProcessingStep(task));
            if (tasksSubList == null) {
                tasksSubList = new LinkedList<>();
                moduleMap.put(displayProcessingStep(task), tasksSubList);
            }

            tasksSubList.add(task);
        }

        for (String moduleName : orderedModuleNames) {
            Map<String, List<PipelineTaskDisplayData>> moduleMap = moduleStats.get(moduleName);

            updateStats(moduleName, moduleMap, ERROR);
            for (ProcessingStep processingStep : ProcessingStep.values()) {
                updateStats(moduleName, moduleMap, processingStep.toString());
            }
        }
    }

    private void updateStats(String moduleName,
        Map<String, List<PipelineTaskDisplayData>> moduleMap, String displayProcessingStep) {
        List<PipelineTaskDisplayData> tasksSubList = moduleMap.get(displayProcessingStep);
        if (tasksSubList != null) {
            TaskProcessingTimeStats s = TaskProcessingTimeStats.of(tasksSubList);
            stats.add(new ProcessingStatistics(moduleName, displayProcessingStep, s));
        }
    }

    private String displayProcessingStep(PipelineTaskDisplayData task) {
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
            case 1 -> formatDate(s.getMinStart());
            case 2 -> statsForTaskType.getDisplayProcessingStep();
            case 3 -> s.getCount();
            case 4 -> formatDouble(s.getSum());
            case 5 -> formatDouble(s.getMin());
            case 6 -> formatDouble(s.getMax());
            case 7 -> formatDouble(s.getMean());
            case 8 -> formatDouble(s.getStddev());
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

    /**
     * Computes the following statistics based on the processing times for the specified list of
     * pipeline tasks:
     *
     * <pre>
     * max
     * min
     * mean
     * stddev
     * </pre>
     *
     * @author Todd Klaus
     */
    static class TaskProcessingTimeStats {
        private int count;
        private double sum;
        private double min;
        private double max;
        private double mean;
        private double stddev;
        private Date minStart = new Date();

        /**
         * Private to prevent instantiation. Use static 'of' method to create instances.
         */
        private TaskProcessingTimeStats() {
        }

        public static TaskProcessingTimeStats of(List<PipelineTaskDisplayData> tasks) {
            TaskProcessingTimeStats s = new TaskProcessingTimeStats();
            DescriptiveStatistics stats = new DescriptiveStatistics();

            for (PipelineTaskDisplayData task : tasks) {
                Date createdTime = task.getCreated();

                if (createdTime.getTime() > 0 && createdTime.getTime() < s.minStart.getTime()) {
                    s.minStart = createdTime;
                }

                stats.addValue(
                    DisplayModel.getProcessingHours(task.getExecutionClock().totalExecutionTime()));
            }

            s.count = tasks.size();
            s.sum = stats.getSum();
            s.min = stats.getMin();
            s.max = stats.getMax();
            s.mean = stats.getMean();
            s.stddev = stats.getStandardDeviation();

            return s;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public double getMean() {
            return mean;
        }

        public double getStddev() {
            return stddev;
        }

        public int getCount() {
            return count;
        }

        public Date getMinStart() {
            return minStart;
        }

        public double getSum() {
            return sum;
        }
    }
}
