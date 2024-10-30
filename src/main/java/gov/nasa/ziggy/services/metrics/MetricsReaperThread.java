package gov.nasa.ziggy.services.metrics;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.MetricsOperations;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

public class MetricsReaperThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(MetricsReaperThread.class);

    private static final String CHECK_INTERVAL_MINS_PROP = "pi.metrics.reaper.checkIntervalMins";
    private static final String MAX_ROWS_PROP = "pi.metrics.reaper.maxRows";

    private static final int DEFAULT_CHECK_INTERVAL_MINS = 5;
    private static final int DEFAULT_MAX_ROWS = 10000;

    int checkIntervalMillis;
    int maxRows;

    private long lastCheck = System.currentTimeMillis();
    MetricsOperations metricsOperations = new MetricsOperations();

    public MetricsReaperThread() {
        super("MetricsReaperThread");
        setPriority(Thread.NORM_PRIORITY + 1);
    }

    @Override
    @AcceptableCatchBlock(rationale = Rationale.SYSTEM_EXIT)
    public void run() {
        try {
            log.info("Starting MetricsReaperThread");

            ImmutableConfiguration config = ZiggyConfiguration.getInstance();
            checkIntervalMillis = config.getInt(CHECK_INTERVAL_MINS_PROP,
                DEFAULT_CHECK_INTERVAL_MINS) * 60 * 1000;
            maxRows = config.getInt(MAX_ROWS_PROP, DEFAULT_MAX_ROWS);

            log.info("maxRows={}, checkIntervalMillis={}", maxRows, checkIntervalMillis);

            while (true) {
                long now = System.currentTimeMillis();
                if (now - lastCheck > checkIntervalMillis) {
                    log.info("Checking rowCount");
                    metricsOperations().deleteOldMetrics(maxRows);
                    lastCheck = now;
                    log.info("Checking rowCount...done");
                }

                Thread.sleep(1000);
            }
        } catch (Throwable t) {
            log.error("Caught exception", t);
            System.exit(-1);
        }
    }

    MetricsOperations metricsOperations() {
        return metricsOperations;
    }
}
