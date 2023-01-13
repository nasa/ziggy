package gov.nasa.ziggy.metrics;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a simple metric consisting of an integer counter
 *
 * @author Todd Klaus
 */
public class CounterMetric extends Metric implements Serializable {
    private static final long serialVersionUID = -5164077933977735823L;

    private final AtomicInteger count = new AtomicInteger(0);

    public static final String COUNTER_TYPE = "C";

    /**
     * @return
     */
    public int getCount() {
        return count.get();
    }

    /**
     * @param metricName
     */
    public static CounterMetric decrement(String metricName) {
        return CounterMetric.decrement(metricName, 1);
    }

    /**
     * @param metricName
     */
    public static CounterMetric decrement(String metricName, int amount) {
        GlobalThreadMetrics<CounterMetric> counterMetric = getCounterMetric(metricName);
        counterMetric.getGlobalMetric().decrement(amount);
        if (counterMetric.getThreadMetric() != null) {
            counterMetric.getThreadMetric().decrement(amount);
        }
        return counterMetric.getGlobalMetric();
    }

    /**
     * @param metricName
     */
    public static CounterMetric increment(String metricName) {
        return CounterMetric.increment(metricName, 1);
    }

    /**
     * @param metricName
     */
    public static CounterMetric increment(String metricName, int amount) {
        GlobalThreadMetrics<CounterMetric> counterMetric = getCounterMetric(metricName);
        counterMetric.getGlobalMetric().increment(amount);
        if (counterMetric.getThreadMetric() != null) {
            counterMetric.getThreadMetric().increment(amount);
        }
        return counterMetric.getGlobalMetric();
    }

    @Override
    public void toLogString(StringBuilder bldr) {
        bldr.append(name).append(',').append(COUNTER_TYPE).append(',').append(count);
    }

    protected CounterMetric(String name) {
        setName(name);
    }

    protected CounterMetric(CounterMetric otherMetric) {
    }

    @Override
    public synchronized void merge(Metric other) {
        if (!(other instanceof CounterMetric)) {
            throw new IllegalArgumentException(
                "Specified Metric is not a CounterMetric, type=" + other.getClass().getName());
        }
        CounterMetric otherCm = (CounterMetric) other;
        count.addAndGet(otherCm.count.get());
    }

    @Override
    public CounterMetric makeCopy() {
        CounterMetric copy = new CounterMetric(name);
        copy.count.set(count.get());
        return copy;
    }

    protected void decrement() {
        count.decrementAndGet();
    }

    protected void decrement(int amount) {
        int oldValue = -1;
        do {
            oldValue = count.get();
        } while (!count.compareAndSet(oldValue, oldValue - amount));
    }

    protected void increment() {
        count.incrementAndGet();
    }

    protected void increment(int amount) {
        int oldValue = -1;
        do {
            oldValue = count.get();
        } while (!count.compareAndSet(oldValue, oldValue + amount));
    }

    @Override
    protected void reset() {
        count.set(0);
    }

    /**
     * @param name
     * @return
     */
    protected static GlobalThreadMetrics<CounterMetric> getCounterMetric(String name) {
        Metric globalMetric = Metric.getGlobalMetric(name);
        if (globalMetric == null || !(globalMetric instanceof CounterMetric)) {
            globalMetric = Metric.addNewGlobalMetric(new CounterMetric(name));
        }

        Metric threadMetric = null;
        if (Metric.threadMetricsEnabled()) {
            threadMetric = Metric.getThreadMetric(name);
            if (threadMetric == null || !(threadMetric instanceof CounterMetric)) {
                threadMetric = Metric.addNewThreadMetric(new CounterMetric(name));
            }
        }

        return new GlobalThreadMetrics<>((CounterMetric) globalMetric,
            (CounterMetric) threadMetric);
    }

    @Override
    public synchronized int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        return prime * result + (count == null ? 0 : count.get());
    }

    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || getClass() != obj.getClass()) {
            return false;
        }
        final CounterMetric other = (CounterMetric) obj;
        if (count == null) {
            if (other.count != null) {
                return false;
            }
        } else if (count.get() != other.count.get()) {
            return false;
        }
        return true;
    }
}
