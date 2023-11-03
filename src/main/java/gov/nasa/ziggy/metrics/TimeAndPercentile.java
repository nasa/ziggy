package gov.nasa.ziggy.metrics;

import java.util.Objects;

public class TimeAndPercentile {

    @Override
    public int hashCode() {
        return Objects.hash(percent, timeMillis);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeAndPercentile other = (TimeAndPercentile) obj;
        return Double.doubleToLongBits(percent) == Double.doubleToLongBits(other.percent)
            && timeMillis == other.timeMillis;
    }

    private final long timeMillis;
    private final double percent;

    public TimeAndPercentile(long timeMillis, double percent) {
        this.timeMillis = timeMillis;
        this.percent = percent;
    }

    public long getTimeMillis() {
        return timeMillis;
    }

    public double getPercent() {
        return percent;
    }
}
