package gov.nasa.ziggy.metrics.report;

import java.text.DecimalFormat;

public class BytesPerSecondFormat implements Format {
    @Override
    public String format(double bytesPerSecond) {
        long value = (long) bytesPerSecond;

        if (value <= 0) {
            return "0";
        }

        final String[] units = new String[] { "B/s", "KB/s", "MB/s", "GB/s", "TB/s" };
        int digitGroups = (int) (Math.log10(value) / Math.log10(1024));

        return new DecimalFormat("#,##0.#").format(value / Math.pow(1024, digitGroups)) + " "
            + units[digitGroups];
    }
}
