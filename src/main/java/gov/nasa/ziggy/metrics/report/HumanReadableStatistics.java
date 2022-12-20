package gov.nasa.ziggy.metrics.report;

import java.util.List;
import java.util.Objects;

/**
 * Container for a {@link List} of double-precision values that have been converted to a
 * human-readable form (i.e., to hours, or minutes, etc., whatever is most convenient), and the
 * {@link String} that indicates the unit of the values.
 *
 * @author PT
 */
public class HumanReadableStatistics {

    private final String unit;
    private final List<Double> values;

    public HumanReadableStatistics(String unit, List<Double> values) {
        this.unit = unit;
        this.values = values;
    }

    public String getUnit() {
        return unit;
    }

    public List<Double> getValues() {
        return values;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        HumanReadableStatistics other = (HumanReadableStatistics) obj;
        return Objects.equals(unit, other.unit) && Objects.equals(values, other.values);
    }
}
