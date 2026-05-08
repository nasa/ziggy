package gov.nasa.ziggy.report;

import java.text.DecimalFormat;

/**
 * A uniform format for all numbers in report. Instances of this class are not thread safe.
 *
 * @author Bill Wohler
 */
public class ReportValueFormat implements Format {

    private static final DecimalFormat FORMAT = new DecimalFormat("#,##0.####");

    @Override
    public String format(double value) {
        return FORMAT.format(value);
    }
}
