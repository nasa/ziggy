package gov.nasa.ziggy.util;

import java.util.Date;
import java.util.Objects;

/**
 * Represents a start and end {@link Date}.
 *
 * @author PT
 */
public class TimeRange {

    private final Date startTimestamp;
    private final Date endTimestamp;

    public TimeRange(Date startTimestamp, Date endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public Date getStartTimestamp() {
        return startTimestamp;
    }

    public Date getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(endTimestamp, startTimestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeRange other = (TimeRange) obj;
        return Objects.equals(endTimestamp, other.endTimestamp)
            && Objects.equals(startTimestamp, other.startTimestamp);
    }
}
