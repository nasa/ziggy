package gov.nasa.ziggy.metrics;

import java.io.Serializable;

/**
 * Implements a metric which represents an accumulator which can be incremented by an arbitrary
 * amount. Also tracks the the minimum and maximum values added and the count of values added,
 * allowing the computation of the average.
 *
 * @author Todd Klaus
 */
public class ValueMetric extends Metric implements Serializable {
    private static final long serialVersionUID = 20230511L;

    public static final String VALUE_TYPE = "V";

    protected long min = Long.MAX_VALUE;
    protected long max = Long.MIN_VALUE;
    protected int count = 0;
    protected long sum = 0;

    public synchronized double getAverage() {
        if (count > 0) {
            return (double) sum / (double) count;
        }
        return 0;
    }

    public synchronized long getSum() {
        return sum;
    }

    public synchronized int getCount() {
        return count;
    }

    public synchronized long getMax() {
        return max == Long.MIN_VALUE ? 0 : max;
    }

    public synchronized long getMin() {
        return min == Long.MAX_VALUE ? 0 : min;
    }

    @Override
    public synchronized void toLogString(StringBuilder bldr) {
        bldr.append(name)
            .append(',')
            .append(VALUE_TYPE)
            .append(',')
            .append(min)
            .append(',')
            .append(max)
            .append(',')
            .append(getAverage())
            .append(',')
            .append(count)
            .append(',')
            .append(sum);
    }

    @Override
    public String toString() {
        StringBuilder bldr = new StringBuilder();
        bldr.append("mean: ")
            .append(getAverage())
            .append(", min: ")
            .append(min)
            .append(", max: ")
            .append(max)
            .append(", count: ")
            .append(count)
            .append(", sum: ")
            .append(sum);
        return bldr.toString();
    }

    public static ValueMetric addValue(String name, long value) {
        GlobalThreadMetrics<ValueMetric> m = getValueMetric(name);
        m.getGlobalMetric().addValue(value);
        if (m.getThreadMetric() != null) {
            m.getThreadMetric().addValue(value);
        }
        return m.getGlobalMetric();
    }

    protected static GlobalThreadMetrics<ValueMetric> getValueMetric(String name) {
        Metric globalMetric = Metric.getGlobalMetric(name);
        if (globalMetric == null || !(globalMetric instanceof ValueMetric)) {
            globalMetric = Metric.addNewGlobalMetric(new ValueMetric(name));
        }

        Metric threadMetric = null;
        if (Metric.threadMetricsEnabled()) {
            threadMetric = Metric.getThreadMetric(name);
            if (threadMetric == null || !(threadMetric instanceof ValueMetric)) {
                threadMetric = addNewThreadMetric(new ValueMetric(name));
            }
        }
        return new GlobalThreadMetrics<>((ValueMetric) globalMetric, (ValueMetric) threadMetric);
    }

    protected ValueMetric(String name) {
        setName(name);
    }

    ValueMetric(String name, long min, long max, int count, long sum) {
        this(name);
        this.min = min;
        this.max = max;
        this.count = count;
        this.sum = sum;
    }

    @Override
    public synchronized ValueMetric makeCopy() {
        ValueMetric copy = new ValueMetric(name);
        copy.name = name;
        copy.sum = sum;
        copy.min = min;
        copy.max = max;
        copy.count = count;
        return copy;
    }

    protected synchronized void addValue(long value) {
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        count++;
        sum += value;
    }

    @Override
    public synchronized void merge(Metric other) {
        if (!(other instanceof ValueMetric)) {
            throw new IllegalArgumentException(
                "Specified Metric is not a ValueMetric, type=" + other.getClass().getName());
        }
        ValueMetric otherVm = (ValueMetric) other;
        count += otherVm.count;
        sum += otherVm.sum;
        max = Math.max(max, otherVm.max);
        min = Math.min(min, otherVm.min);
    }

    @Override
    protected synchronized void reset() {
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
        count = 0;
        sum = 0;
    }

    @Override
    public synchronized int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + count;
        result = prime * result + (int) (max ^ max >>> 32);
        result = prime * result + (int) (min ^ min >>> 32);
        return prime * result + (int) (sum ^ sum >>> 32);
    }

    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || getClass() != obj.getClass()) {
            return false;
        }
        final ValueMetric other = (ValueMetric) obj;
        if (count != other.count || max != other.max || min != other.min || sum != other.sum) {
            return false;
        }
        return true;
    }
}
