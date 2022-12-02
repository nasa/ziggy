package gov.nasa.ziggy.metrics.report;

import org.apache.commons.lang3.time.DurationFormatUtils;

public class TimeMillisFormat implements Format {
    @Override
    public String format(double timeMillis) {
        return DurationFormatUtils.formatDuration((long) timeMillis, "HH:mm:ss");
    }
}
