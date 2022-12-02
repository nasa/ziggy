package gov.nasa.ziggy.metrics;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "PI_METRIC_TYPE")
public class MetricType implements Comparable<MetricType> {
    public static final int TYPE_COUNTER = 1;
    public static final int TYPE_VALUE = 2;

    @Id
    private String name;

    private int type;

    public MetricType() {
    }

    /**
     * @param name
     * @param type
     */
    public MetricType(String name, int type) {
        if (name == null) {
            throw new NullPointerException("Parameter \"name\" was null.");
        }
        switch (type) {
            case TYPE_COUNTER:
            case TYPE_VALUE:
                break;
            default:
                throw new IllegalArgumentException("Bad value for MetricType's type " + type + ".");
        }
        this.name = name;
        this.type = type;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Returns the name.
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @param name The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(MetricType o) {
        return name.compareTo(o.getName());
    }

    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof MetricType)) {
            return false;
        }
        MetricType otherMetricType = (MetricType) other;
        return name.equals(otherMetricType.name);
    }
}
