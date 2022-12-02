package gov.nasa.ziggy.pipeline.definition;

import javax.persistence.Embeddable;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

/**
 * @author Todd Klaus
 */
@Embeddable
public class PipelineTaskMetrics {
    public enum Units {
        TIME, BYTES, RATE
    }

    private String category;

    private long value = 0;

    @Enumerated(EnumType.STRING)
    private Units units;

    public PipelineTaskMetrics() {
    }

    public PipelineTaskMetrics(String category, long value, Units units) {
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
}
