package gov.nasa.ziggy.metrics;

import java.util.Objects;

/**
 * Wrapper around the IntervalMetric String key to allow for future expansion (like user-supplied
 * keys)
 *
 * @author Todd Klaus
 */
public class IntervalMetricKey {
    private String key = null;

    private long startTime = 0;

    /**
     * @param _key
     */
    public IntervalMetricKey(String _key) {
        key = _key;
    }

    /**
     * @param startTime
     */
    public IntervalMetricKey(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return
     */
    public String getKey() {
        return key;
    }

    /**
     * @param string
     */
    public void setKey(String string) {
        key = string;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IntervalMetricKey other = (IntervalMetricKey) obj;
        if (!Objects.equals(key, other.key)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return key;
    }

    /**
     * @return
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param l
     */
    public void setStartTime(long l) {
        startTime = l;
    }
}
