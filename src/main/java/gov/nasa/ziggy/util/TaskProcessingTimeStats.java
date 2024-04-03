package gov.nasa.ziggy.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.util.dispmod.DisplayModel;

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
public class TaskProcessingTimeStats {
    private int count;
    private double sum;
    private double min;
    private double max;
    private double mean;
    private double stddev;
    private Date minStart = new Date();
    private Date maxEnd = new Date(0);
    private double totalElapsed;

    /**
     * Private to prevent instantiation. Use static 'of' method to create instances.
     *
     * @param tasks
     */
    private TaskProcessingTimeStats() {
    }

    public static TaskProcessingTimeStats of(List<PipelineTask> tasks) {
        TaskProcessingTimeStats s = new TaskProcessingTimeStats();

        List<Double> processingTimesHrs = new ArrayList<>(tasks.size());

        for (PipelineTask task : tasks) {
            Date startProcessingTime = task.getStartProcessingTime();
            Date endProcessingTime = task.getEndProcessingTime();

            if (startProcessingTime.getTime() > 0
                && startProcessingTime.getTime() < s.minStart.getTime()) {
                s.minStart = startProcessingTime;
            }

            if (endProcessingTime.getTime() > 0
                && endProcessingTime.getTime() > s.maxEnd.getTime()) {
                s.maxEnd = endProcessingTime;
            }

            processingTimesHrs
                .add(DisplayModel.getProcessingHours(startProcessingTime, endProcessingTime));
        }

        s.totalElapsed = DisplayModel.getProcessingHours(s.minStart, s.maxEnd);

        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (Double d : processingTimesHrs) {
            stats.addValue(d);
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

    public Date getMaxEnd() {
        return maxEnd;
    }

    public double getTotalElapsed() {
        return totalElapsed;
    }

    public double getSum() {
        return sum;
    }
}
