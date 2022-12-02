package gov.nasa.ziggy.worker;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * @author Todd Klaus
 */
public class RandomDelay {
    private static final Logger log = LoggerFactory.getLogger(RandomDelay.class);

    private static final String RANDOM_DELAY_PROP = "pi.worker.randomDelaySeconds";
    private static final int RANDOM_DELAY_DEFAULT = 0;

    // private to prevent instantiation
    private RandomDelay() {
    }

    public static void randomWait() {
        Configuration config = ZiggyConfiguration.getInstance();
        int maxDelaySeconds = config.getInt(RANDOM_DELAY_PROP, RANDOM_DELAY_DEFAULT);

        if (maxDelaySeconds > 0) {
            int delayMillis = (int) (Math.random() * maxDelaySeconds * 1000);
            log.info("Sleeping for " + delayMillis + " milliseconds to stagger transaction start");
            try {
                Thread.sleep(delayMillis);
            } catch (Throwable t) {
            }
        } else {
            log.info("No delay configured");
        }
    }
}
