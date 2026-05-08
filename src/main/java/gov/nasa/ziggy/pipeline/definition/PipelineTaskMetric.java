package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

/**
 * A performance metric that is associated with a single {@link PipelineTask}.
 *
 * @author Todd Klaus
 * @author PT
 */
@Embeddable
public class PipelineTaskMetric implements Comparable<PipelineTaskMetric> {
    public enum Unit {
        MILLIS, BYTES;

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    public enum Metric {
        MARSHALING_TIME {
            @Override
            public Unit unit() {
                return Unit.MILLIS;
            }
        },
        QUEUE_TIME {
            @Override
            public Unit unit() {
                return Unit.MILLIS;
            }
        },
        ALGORITHM_TIME {
            @Override
            public Unit unit() {
                return Unit.MILLIS;
            }
        },
        PERSISTING_TIME {
            @Override
            public Unit unit() {
                return Unit.MILLIS;
            }
        },
        INPUTS_SIZE {
            @Override
            public Unit unit() {
                return Unit.BYTES;
            }
        },
        OUTPUTS_SIZE {
            @Override
            public Unit unit() {
                return Unit.BYTES;
            }
        };

        public abstract Unit unit();
    }

    @Enumerated(EnumType.STRING)
    private Metric metric;

    private long value = 0;

    public PipelineTaskMetric() {
    }

    public PipelineTaskMetric(Metric metric) {
        this.metric = metric;
    }

    /**
     * Update the metric value. For MILLIS metrics, the value is incremented by the method argument;
     * for BYTES metric, the method argument replaces the value in the {@link PipelineTaskMetric}.
     */
    public void updateValue(long newValue) {
        switch (metric.unit()) {
            case BYTES:
                value = newValue;
                return;
            case MILLIS:
                value += newValue;
                return;
            default:
                throw new IllegalStateException("Unknown unit " + metric.unit());
        }
    }

    public Metric getMetric() {
        return metric;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return metric.name() + " = " + value + " " + metric.unit();
    }

    // The equals() and hashCode() methods look only at the Metric so that we can use contains() on
    // a collection to see whether a given PipelineTaskMetric is represented within the collection.

    @Override
    public int hashCode() {
        return Objects.hash(metric);
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
        return Objects.equals(metric, other.getMetric());
    }

    @Override
    public int compareTo(PipelineTaskMetric o) {
        return metric.compareTo(o.getMetric());
    }
}
