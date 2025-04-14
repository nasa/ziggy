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

    private static final String ERROR = "ERROR";

    private List<ProcessingStatistics> stats = new LinkedList<>();

    public PipelineStatsDisplayModel(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames) {
        update(tasks, orderedPipelineStepNames);
    }

    private void update(List<PipelineTaskDisplayData> tasks,
        List<String> orderedPipelineStepNames) {
        stats = new LinkedList<>();

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
                tasksSubList = new LinkedList<>();
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
            TaskProcessingTimeStats s = TaskProcessingTimeStats.of(tasksSubList);
            stats.add(new ProcessingStatistics(pipelineStepName, processingStep, s));
        }
    }

    private String processingStep(PipelineTaskDisplayData task) {
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
            case 0 -> statsForTaskType.getPipelineStepName();
            case 1 -> formatDate(s.getMinStart());
            case 2 -> statsForTaskType.getProcessingStep();
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
