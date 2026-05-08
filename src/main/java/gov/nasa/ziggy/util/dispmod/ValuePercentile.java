package gov.nasa.ziggy.util.dispmod;

import java.util.Objects;

public class ValuePercentile {

    private final long value;
    private final double percent;

    public ValuePercentile(long value, double percent) {
        this.value = value;
        this.percent = percent;
    }

    public long getValue() {
        return value;
    }

    public double getPercent() {
        return percent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(percent, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ValuePercentile other = (ValuePercentile) obj;
        return Double.doubleToLongBits(percent) == Double.doubleToLongBits(other.percent)
            && value == other.value;
    }
}
