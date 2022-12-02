package gov.nasa.ziggy.metrics;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name = "PI_METRIC_VALUE")
public class MetricValue {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_METRIC_TYPE_SEQ",
        allocationSize = 1)
    private long id;

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
    public long getId() {
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
