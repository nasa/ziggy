package gov.nasa.ziggy.services.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskLogCreator {

    public static final Logger log = LoggerFactory.getLogger(TaskLogCreator.class);

    public static void main(String[] args) {
        log.info(args[0]);
    }
}
