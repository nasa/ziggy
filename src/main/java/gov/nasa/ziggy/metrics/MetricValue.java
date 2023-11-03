package gov.nasa.ziggy.metrics;

import java.util.Date;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

@Entity
@Table(name = "ziggy_MetricValue")
public class MetricValue {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_MetricValue_generator")
    @SequenceGenerator(name = "ziggy_MetricValue_generator", initialValue = 1,
        sequenceName = "ziggy_MetricValue_sequence", allocationSize = 1)
    private Long id;

    @ManyToOne
    private MetricType metricType;

    private Date timestamp;
    private String source;
    private float value;

    public MetricValue() {
    }

    /**
     * @param source this is where the metric originated, like from a particular host or worker
     * thread.
     * @param metricType
     * @param timestamp
     * @param value
     */
    public MetricValue(String source, MetricType metricType, Date timestamp, float value) {
        this.source = source;
        this.metricType = metricType;
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * @return Returns the source.
     */
    public String getSource() {
        return source;
    }

    /**
     * @param sourceHost The source to set.
     */
    public void setSource(String sourceHost) {
        source = sourceHost;
    }

    /**
     * @return Returns the timestamp.
     */
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * @param time The timestamp to set.
     */
    public void setTimestamp(Date time) {
        timestamp = time;
    }

    /**
     * @return the value
     */
    public float getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(float value) {
        this.value = value;
    }

    /**
     * @return the id
     */
    public Long getId() {
        return id;
    }

    /**
     * @return the metricType
     */
    public MetricType getMetricType() {
        return metricType;
    }

    @Override
    public String toString() {
        return "metricType=[" + metricType + "], source=[" + source + "], timestamp=[" + timestamp
            + "], value=[" + value + "]";
    }
}
