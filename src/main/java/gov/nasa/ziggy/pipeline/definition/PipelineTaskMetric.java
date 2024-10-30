package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

/**
 * @author Todd Klaus
 */
@Embeddable
public class PipelineTaskMetric {
    public enum Units {
        TIME, BYTES, RATE
    }

    private String category;

    private long value = 0;

    @Enumerated(EnumType.STRING)
    private Units units;

    public PipelineTaskMetric() {
    }

    public PipelineTaskMetric(String category, long value, Units units) {
        this.category = category;
        this.value = value;
        this.units = units;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long timeMillis) {
        value = timeMillis;
    }

    public Units getUnits() {
        return units;
    }

    public void setUnits(Units units) {
        this.units = units;
    }

    @Override
    public String toString() {
        return "category: " + category + ", timeMillis: " + value + ", units: " + units;
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, units, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineTaskMetric other = (PipelineTaskMetric) obj;
        return Objects.equals(category, other.category) && units == other.units
            && value == other.value;
    }
}
