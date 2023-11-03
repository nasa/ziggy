package gov.nasa.ziggy.ui.metrilyzer;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeSet;

import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricValue;

/**
 * Holds a list of {@link MetricValue}s and provides binning functionality
 *
 * @author Todd Klaus
 */
public class SampleList {
    private static final Logger log = LoggerFactory.getLogger(SampleList.class);

    private class Sample implements Comparable<Sample> {
        public long time = 0;
        public double value = 0.0;

        public Sample(long time, double value) {
            this.time = time;
            this.value = value;
        }

        @Override
        public int compareTo(Sample o) {
            return (int) (time - o.time);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            return prime * result + (int) (time ^ time >>> 32);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Sample other = (Sample) obj;
            if (!getOuterType().equals(other.getOuterType()) || time != other.time) {
                return false;
            }
            return true;
        }

        private SampleList getOuterType() {
            return SampleList.this;
        }
    }

    private TreeSet<Sample> samples = new TreeSet<>();

    /**
     *
     *
     */
    public SampleList() {
    }

    /**
     * Add the given metrics to the existing Sample list. Assumes all metrics in the given list are
     * from the same JVM.
     *
     * @param metrics
     */
    public void ingest(Collection<MetricValue> metrics) {
        for (MetricValue metricValue : metrics) {
            addSample(metricValue.getTimestamp().getTime(), metricValue.getValue());
        }
    }

    /**
     * Add a sample to the list. If a sample already exists with the same timestamp, it is replaced.
     * This results in some loss of data, but it should be rare to have two samples with the exact
     * same timestamp.
     *
     * @param timestamp
     * @param value
     */
    public void addSample(long timestamp, double value) {
        Sample newSample = new Sample(timestamp, value);
        samples.add(newSample);
    }

    /**
     * @param binSizeMillis
     */
    public void bin(long binSizeMillis) {
        if (samples.size() == 0) {
            log.warn("sample list empty!  No bin for you!");
            return;
        }

        Iterator<Sample> sampleIterator = samples.iterator();
        Sample currentSample = sampleIterator.next();
        TreeSet<Sample> newList = new TreeSet<>();
        long thisBinStart = samples.first().time;
        long lastSampleTime = samples.last().time;
        long nextBinStart = thisBinStart + binSizeMillis;
        long currentBinMid = (thisBinStart + nextBinStart) / 2;
        double sum = 0.0;
        double count = 0.0;

        log.info("START binning, input set size=" + samples.size());

        while (thisBinStart < lastSampleTime) {
            if (currentSample.time >= nextBinStart) {
                // end of bin
                log.debug("end of bin, count=" + count);
                if (count > 0.0) {
                    // at least one sample in this bin, store the average
                    log.debug("adding sample @" + new Date(currentBinMid) + " = " + sum / count);
                    newList.add(new Sample(currentBinMid, sum / count));
                }
                thisBinStart = nextBinStart;
                nextBinStart = thisBinStart + binSizeMillis;
                currentBinMid = (thisBinStart + nextBinStart) / 2;
                log.debug("new bin start = " + new Date(thisBinStart));
                sum = 0.0;
                count = 0.0;
            } else {
                sum += currentSample.value;
                count += 1.0;
                if (!sampleIterator.hasNext()) {
                    break; // no more samples
                }
                currentSample = sampleIterator.next();
            }
        }

//        for (Sample sample : samples) {
//            log.debug("sample.time = " + new Date(sample.time));
//            if (sample.time >= nextBinStart) {
//                // end of bin
//                log.debug("end of bin, count=" + count);
//                if (count > 0.0) {
//                    // at least one sample in this bin, store the average
//                    log.debug("adding sample @" + new Date(currentBinMid) + " = " + sum / count);
//                    newList.add(new Sample(currentBinMid, sum / count));
//                }
//                thisBinStart = nextBinStart;
//                nextBinStart = thisBinStart + binSizeMillis;
//                currentBinMid = (thisBinStart + nextBinStart) / 2;
//                log.debug("new bin start = " + new Date(thisBinStart));
//                sum = 0.0;
//                count = 0.0;
//            }
//
//            sum += sample.value;
//            count += 1.0;
//        }

        log.info("END binning, output set size=" + newList.size());

        samples = newList;
    }

    /**
     * @param name
     * @return
     */
    public TimeSeries asTimeSeries(String name) {
        @SuppressWarnings("deprecation")
        TimeSeries series = new TimeSeries(name, Millisecond.class);

        for (Sample sample : samples) {
            series.addOrUpdate(new Millisecond(new Date(sample.time)), sample.value);
        }

        return series;
    }

    public int size() {
        return samples.size();
    }
}
