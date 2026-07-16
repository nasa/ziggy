package gov.nasa.ziggy.util.dispmod;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
 * Sum, max, min, mean, and standard deviation are provided for each node/processingStep
 * combination.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class PipelineStatsDisplayModel extends DisplayModel
    implements ModelContentClass<ProcessingStatistics> {

    private static final String[] COLUMN_NAMES = { "Node", "Start", "Status", "Count", "Sum (hrs)",
        "Min (hrs)", "Max (hrs)", "Mean (hrs)", "Std (hrs)" };
    private static final Class<?>[] COLUMN_CLASSES = { String.class, Date.class, String.class,
        Integer.class, Double.class, Double.class, Double.class, Double.class, Double.class };

    private static final String ERROR = "ERROR";

    private List<ProcessingStatistics> statistics = new ArrayList<>();

    public PipelineStatsDisplayModel(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames) {
        update(tasks, orderedPipelineStepNames);
    }

    private void update(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames) {

        Map<String, Map<String, List<PipelineTaskDisplayData>>> tasksSubListByProcessingStepByStep = new HashMap<>();

        for (PipelineTaskDisplayData task : tasks) {
            String pipelineStepName = task.getPipelineStepName();

            Map<String, List<PipelineTaskDisplayData>> tasksSubListByProcessingStep = tasksSubListByProcessingStepByStep
                .get(pipelineStepName);
            if (tasksSubListByProcessingStep == null) {
                tasksSubListByProcessingStep = new HashMap<>();
                tasksSubListByProcessingStepByStep.put(pipelineStepName,
                    tasksSubListByProcessingStep);
            }

            List<PipelineTaskDisplayData> tasksSubList = tasksSubListByProcessingStep
                .get(processingStep(task));
            if (tasksSubList == null) {
                tasksSubList = new ArrayList<>();
                tasksSubListByProcessingStep.put(processingStep(task), tasksSubList);
            }

            tasksSubList.add(task);
        }

        for (String pipelineStepName : orderedPipelineStepNames) {
            Map<String, List<PipelineTaskDisplayData>> tasksSubListByProcessingStep = tasksSubListByProcessingStepByStep
                .get(pipelineStepName);

            updateStats(pipelineStepName, tasksSubListByProcessingStep, ERROR);
            for (ProcessingStep processingStep : ProcessingStep.values()) {
                updateStats(pipelineStepName, tasksSubListByProcessingStep,
                    processingStep.toString());
            }
        }
    }

    private void updateStats(String pipelineStepName,
        Map<String, List<PipelineTaskDisplayData>> tasksSubListByProcessingStep,
        String processingStep) {

        List<PipelineTaskDisplayData> tasksSubList = tasksSubListByProcessingStep
            .get(processingStep);
        if (tasksSubList != null) {
            statistics.add(new ProcessingStatistics(pipelineStepName, processingStep,
                TaskProcessingTimeStats.of(tasksSubList)));
        }
    }

    private String processingStep(PipelineTaskDisplayData task) {
        return task.isError() ? ERROR : task.getProcessingStep().toString();
    }

    @Override
    public int getRowCount() {
        return statistics.size();
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
        ProcessingStatistics statsForTaskType = statistics.get(rowIndex);
        TaskProcessingTimeStats stats = statsForTaskType.getProcessingStats();

        return switch (columnIndex) {
            case 0 -> statsForTaskType.getPipelineStepName();
            case 1 -> stats.getMinStart();
            case 2 -> statsForTaskType.getProcessingStep();
            case 3 -> stats.getCount();
            case 4 -> stats.getSum();
            case 5 -> stats.getMin();
            case 6 -> stats.getMax();
            case 7 -> stats.getMean();
            case 8 -> stats.getStddev();
            default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        };
    }

    @Override
    public Class<ProcessingStatistics> tableModelContentClass() {
        return ProcessingStatistics.class;
    }

    public static class ProcessingStatistics {

        private final String pipelineStepName;
        private final String processingStep;
        private final TaskProcessingTimeStats processingStats;

        public ProcessingStatistics(String pipelineStepName, String processingStep,
            TaskProcessingTimeStats processingStats) {
            this.pipelineStepName = pipelineStepName;
            this.processingStep = processingStep;
            this.processingStats = processingStats;
        }

        public String getPipelineStepName() {
            return pipelineStepName;
        }

        public String getProcessingStep() {
            return processingStep;
        }

        public TaskProcessingTimeStats getProcessingStats() {
            return processingStats;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineStepName, processingStats, processingStep);
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
            return Objects.equals(pipelineStepName, other.pipelineStepName)
                && Objects.equals(processingStats, other.processingStats)
                && processingStep == other.processingStep;
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
            TaskProcessingTimeStats stats = new TaskProcessingTimeStats();
            DescriptiveStatistics statistics = new DescriptiveStatistics();

            for (PipelineTaskDisplayData task : tasks) {
                Date createdTime = task.getCreated();

                if (createdTime.getTime() > 0 && createdTime.getTime() < stats.minStart.getTime()) {
                    stats.minStart = createdTime;
                }

                statistics.addValue(
                    DisplayModel.getProcessingHours(task.getExecutionClock().totalExecutionTime()));
            }

            stats.count = tasks.size();
            stats.sum = statistics.getSum();
            stats.min = statistics.getMin();
            stats.max = statistics.getMax();
            stats.mean = statistics.getMean();
            stats.stddev = statistics.getStandardDeviation();

            return stats;
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
