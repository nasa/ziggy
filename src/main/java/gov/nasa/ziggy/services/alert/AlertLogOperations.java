package gov.nasa.ziggy.services.alert;

import java.util.List;

import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Operations class for alerts. */
public class AlertLogOperations extends DatabaseOperations {

    private AlertLogCrud alertLogCrud = new AlertLogCrud();

    public void persist(AlertLog alert) {
        performTransaction(() -> alertLogCrud().persist(alert));
    }

    public List<AlertLog> alertLogs(long pipelineInstanceId) {
        return performTransaction(
            () -> alertLogCrud().retrieveForPipelineInstance(pipelineInstanceId));
    }

    AlertLogCrud alertLogCrud() {
        return alertLogCrud;
    }
}
