package gov.nasa.ziggy.metrics.report;

public class FloatFormat implements Format {
    @Override
    public String format(double value) {
        return String.format("%.1f", value);
    }
}
