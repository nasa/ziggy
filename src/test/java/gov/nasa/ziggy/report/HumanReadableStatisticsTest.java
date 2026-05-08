package gov.nasa.ziggy.report;

import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.GIGABYTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.HOURS;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.KILOBYTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.MEGABYTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.MINUTES;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.SECONDS;
import static gov.nasa.ziggy.report.HumanReadableStatistics.Unit.TERABYTES;
import static org.junit.Assert.assertEquals;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;

public class HumanReadableStatisticsTest {

    private static final double[] MILLIS_IN_MILLIS = { 1, 2, 3 };
    private static final double[] SECONDS_IN_MILLIS = { 1000, 2000, 3000 };
    private static final double[] MINUTES_IN_MILLIS = { 60000, 120000, 180000 };
    private static final double[] HOURS_IN_MILLIS = { 3600000, 7200000, 10800000 };

    private static final double[] BYTES_IN_BYTES = { 1, 2, 3 };
    private static final double[] KILOBYTES_IN_BYTES = { 1000, 2000, 3000 };
    private static final double[] MEGABYTES_IN_BYTES = { 1_000_000, 2_000_000, 3_000_000 };
    private static final double[] GIGABYTES_IN_BYTES = { 1_000_000_000, 2_000_000_000,
        3_000_000_000D };
    private static final double[] TERABYTES_IN_BYTES = { 1_000_000_000_000D, 2_000_000_000_000D,
        3_000_000_000_000D };

    @Test
    public void testMillisToHumanReadableStats() {
        DescriptiveStatistics stats = new DescriptiveStatistics(MILLIS_IN_MILLIS);
        HumanReadableStatistics humanReadableStats = HumanReadableStatistics
            .millisToHumanReadable(stats);
        assertEquals(SECONDS, humanReadableStats.getUnit());
        assertEquals(0.002, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(0.002, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(SECONDS_IN_MILLIS);
        humanReadableStats = HumanReadableStatistics.millisToHumanReadable(stats);
        assertEquals(SECONDS, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(MINUTES_IN_MILLIS);
        humanReadableStats = HumanReadableStatistics.millisToHumanReadable(stats);
        assertEquals(MINUTES, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(HOURS_IN_MILLIS);
        humanReadableStats = HumanReadableStatistics.millisToHumanReadable(stats);
        assertEquals(HOURS, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);
    }

    @Test
    public void testBytesToHumanReadableStats() {
        DescriptiveStatistics stats = new DescriptiveStatistics(BYTES_IN_BYTES);
        HumanReadableStatistics humanReadableStats = HumanReadableStatistics
            .bytesToHumanReadable(stats);
        assertEquals(KILOBYTES, humanReadableStats.getUnit());
        assertEquals(0.002, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(0.002, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(KILOBYTES_IN_BYTES);
        humanReadableStats = HumanReadableStatistics.bytesToHumanReadable(stats);
        assertEquals(KILOBYTES, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(MEGABYTES_IN_BYTES);
        humanReadableStats = HumanReadableStatistics.bytesToHumanReadable(stats);
        assertEquals(MEGABYTES, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(GIGABYTES_IN_BYTES);
        humanReadableStats = HumanReadableStatistics.bytesToHumanReadable(stats);
        assertEquals(GIGABYTES, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);

        stats = new DescriptiveStatistics(TERABYTES_IN_BYTES);
        humanReadableStats = HumanReadableStatistics.bytesToHumanReadable(stats);
        assertEquals(TERABYTES, humanReadableStats.getUnit());
        assertEquals(2, humanReadableStats.getStatistics().getMean(), 0.0);
        assertEquals(2, humanReadableStats.getValues().get(1), 0.0);
    }

    @Test
    public void testMillisToHumanReadableUnits() {
        assertEquals(3600, HumanReadableStatistics
            .millisToHumanReadableUnits(HumanReadableStatistics.MILLIS_PER_HOUR, SECONDS), 0);
        assertEquals(60, HumanReadableStatistics
            .millisToHumanReadableUnits(HumanReadableStatistics.MILLIS_PER_HOUR, MINUTES), 0);
        assertEquals(1, HumanReadableStatistics
            .millisToHumanReadableUnits(HumanReadableStatistics.MILLIS_PER_HOUR, HOURS), 0);
    }

    @Test
    public void testHoursToHumanReadableUnits() {
        assertEquals(3600, HumanReadableStatistics.hoursToHumanReadableUnits(1, SECONDS), 0);
        assertEquals(60, HumanReadableStatistics.hoursToHumanReadableUnits(1, MINUTES), 0);
        assertEquals(1, HumanReadableStatistics.hoursToHumanReadableUnits(1, HOURS), 0);
    }

    @Test
    public void testGigabytesToHumanReadableUnits() {
        assertEquals(1000000000,
            HumanReadableStatistics.gigabytesToHumanReadableUnits(1000, KILOBYTES), 0);
        assertEquals(1000000,
            HumanReadableStatistics.gigabytesToHumanReadableUnits(1000, MEGABYTES), 0);
        assertEquals(1000, HumanReadableStatistics.gigabytesToHumanReadableUnits(1000, GIGABYTES),
            0);
        assertEquals(1, HumanReadableStatistics.gigabytesToHumanReadableUnits(1000, TERABYTES), 0);
    }
}
