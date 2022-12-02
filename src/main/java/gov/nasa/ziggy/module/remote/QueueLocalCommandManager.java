package gov.nasa.ziggy.module.remote;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * A {@link QueueCommandManager} for issuing queue commands locally.
 *
 * @author PT
 */
public class QueueLocalCommandManager extends QueueCommandManager {

    private static final Logger log = LoggerFactory.getLogger(QueueLocalCommandManager.class);

    private static final String QSTAT = "qstat ";
    private static final String QDEL = "qdel ";

    @Override
    protected List<String> qstat(String commandArgs, String... targets) {

        List<String> qstatResults = new ArrayList<>();
        ExternalProcess p = ExternalProcess.simpleExternalProcess(QSTAT + commandArgs);
        p.logStdErr(false);
        p.logStdOut(false);
        p.writeStdErr(true);
        p.writeStdOut(true);
        try {
            p.run(true, 0);
            qstatResults = p.stdout(targets);
        } catch (Exception e) {
            log.error("Error when attempting to run qstat command", e);
        }
        return qstatResults;
    }

    @Override
    protected void qdel(String qdelArgs) {
        ExternalProcess p = ExternalProcess.simpleExternalProcess(QDEL + qdelArgs);
        log.info("Executing command: " + qdelArgs);
        p.run(false, 0);
    }

}
