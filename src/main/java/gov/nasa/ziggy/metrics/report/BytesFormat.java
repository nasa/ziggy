package gov.nasa.ziggy.metrics.report;

import java.text.DecimalFormat;

public class BytesFormat implements Format {
    @Override
    public String format(double bytes) {
        long value = (long) bytes;

        if (value <= 0) {
            return "0";
        }

        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(value) / Math.log10(1024));

        return new DecimalFormat("#,##0.#").format(value / Math.pow(1024, digitGroups)) + " "
            + units[digitGroups];
    }
}
