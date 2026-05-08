package gov.nasa.ziggy.report;

import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.GIGABYTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.HOURS;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.KILOBYTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.MEGABYTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.MINUTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.SECONDS;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.TERABYTES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Container for a {@link DescriptiveStatistics} object that has been converted to a human-readable
 * form (for example, from milliseconds to hours, or bytes to gigabytes, whatever is most
 * convenient), and the {@link String} that indicates the unit of the values.
 *
 * @author PT
 * @author Bill Wohler
 */
public class HumanReadableStatistics {

    public enum Unit {
        SECONDS("s"),
        MINUTES("m"),
        HOURS("h"),
        KILOBYTES("kB"),
        MEGABYTES("MB"),
        GIGABYTES("GB"),
        TERABYTES("TB");

        private String string;

        Unit(String string) {
            this.string = string;
        }

        public boolean isTime() {
            return this == SECONDS || this == MINUTES || this == HOURS;
        }

        public boolean isSize() {
            return !isTime();
        }

        @Override
        public String toString() {
            return string;
        }
    }

    /**
     * A record that holds the unit and divisor to convert a source value to that unit. Use
     * {@link #fromBytes(double)} or {@link #fromBytes(double)} with a mean value to create a
     * applicable record.
     */
    public record Conversion(Unit unit, double divisor) {
        public static Conversion fromMillis(double value) {
            Unit unit = SECONDS;
            double divisor = MILLIS_PER_SECOND;

            if (value > MILLIS_PER_HOUR) {
                unit = HOURS;
                divisor = MILLIS_PER_HOUR;
            } else if (value > MILLIS_PER_MINUTE) {
                unit = MINUTES;
                divisor = MILLIS_PER_MINUTE;
            }
            return new Conversion(unit, divisor);
        }

        public static Conversion fromBytes(double value) {
            Unit unit = KILOBYTES;
            double divisor = BYTES_PER_KILOBYTE;

            if (value > BYTES_PER_TERABYTE) {
                unit = TERABYTES;
                divisor = BYTES_PER_TERABYTE;
            } else if (value > BYTES_PER_GIGABYTE) {
                unit = GIGABYTES;
                divisor = BYTES_PER_GIGABYTE;
            } else if (value > BYTES_PER_MEGABYTE) {
                unit = MEGABYTES;
                divisor = BYTES_PER_MEGABYTE;
            }
            return new Conversion(unit, divisor);
        }
    }

    public static final double MILLIS_PER_SECOND = 1000;
    public static final double MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60;
    public static final double MILLIS_PER_HOUR = MILLIS_PER_MINUTE * 60;
    public static final double SECONDS_PER_HOUR = 60 * 60;
    public static final double MINUTES_PER_HOUR = 60;

    public static final double BYTES_PER_KILOBYTE = 1.0e3;
    public static final double BYTES_PER_MEGABYTE = 1.0e6;
    public static final double BYTES_PER_GIGABYTE = 1.0e9;
    public static final double BYTES_PER_TERABYTE = 1.0e12;

    private final Unit unit;
    private final DescriptiveStatistics statistics;

    public HumanReadableStatistics(Unit unit, List<Double> values) {
        this.unit = unit;

        statistics = new DescriptiveStatistics();
        values.stream().forEach(v -> statistics.addValue(v));
    }

    public static HumanReadableStatistics millisToHumanReadable(DescriptiveStatistics stats) {
        return toHumanReadable(Conversion.fromMillis(stats.getMean()), stats.getValues());
    }

    public static HumanReadableStatistics bytesToHumanReadable(DescriptiveStatistics stats) {
        return toHumanReadable(Conversion.fromBytes(stats.getMean()), stats.getValues());
    }

    private static HumanReadableStatistics toHumanReadable(Conversion conversion, double[] values) {
        List<Double> list = new ArrayList<>(values.length);
        for (double v : values) {
            double convertedValue = v / conversion.divisor();
            list.add(convertedValue);
        }

        return new HumanReadableStatistics(conversion.unit(), list);
    }

    public static double hoursToHumanReadableUnits(double hours, Unit unit) {
        return switch (unit) {
            case SECONDS -> hours * SECONDS_PER_HOUR;
            case MINUTES -> hours * MINUTES_PER_HOUR;
            case HOURS -> hours;
            default -> hours;
        };
    }

    public static double millisToHumanReadableUnits(double value, Unit unit) {
        return switch (unit) {
            case SECONDS -> value / MILLIS_PER_SECOND;
            case MINUTES -> value / MILLIS_PER_MINUTE;
            case HOURS -> value / MILLIS_PER_HOUR;
            default -> value;
        };
    }

    public static double gigabytesToHumanReadableUnits(double gigabytes, Unit unit) {
        return switch (unit) {
            case KILOBYTES -> gigabytes * BYTES_PER_GIGABYTE / BYTES_PER_KILOBYTE;
            case MEGABYTES -> gigabytes * BYTES_PER_GIGABYTE / BYTES_PER_MEGABYTE;
            case GIGABYTES -> gigabytes;
            case TERABYTES -> gigabytes * BYTES_PER_GIGABYTE / BYTES_PER_TERABYTE;
            default -> gigabytes;
        };
    }

    public Unit getUnit() {
        return unit;
    }

    public DescriptiveStatistics getStatistics() {
        return statistics;
    }

    public List<Double> getValues() {
        return Arrays.stream(statistics.getValues()).boxed().toList();
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit, statistics);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HumanReadableStatistics other = (HumanReadableStatistics) obj;
        return Objects.equals(unit, other.unit) && Objects.equals(statistics, other.statistics);
    }
}
