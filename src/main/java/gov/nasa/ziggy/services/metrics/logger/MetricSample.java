package gov.nasa.ziggy.services.metrics.logger;

import java.util.Date;

import gov.nasa.ziggy.metrics.Metric;

public class MetricSample {
    private final String metricName;
    private final Class<? extends Metric> metricClass;
    private final String source;
    private final Date timestamp;
    private final float value;

    public MetricSample(String metricName, Class<? extends Metric> metricClass, String source,
        Date timestamp, float value) {
        super();
        this.metricName = metricName;
        this.metricClass = metricClass;
        this.source = source;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getSource() {
        return source;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public float getValue() {
        return value;
    }

    public Class<? extends Metric> getMetricClass() {
        return metricClass;
    }

    @Override
    public String toString() {
        return "metricName=[" + metricName + "], source=[" + source + "], timestamp=[" + timestamp
            + "], value=[" + value + "]";
    }
}
