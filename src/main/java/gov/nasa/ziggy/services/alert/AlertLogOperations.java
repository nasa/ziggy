package gov.nasa.ziggy.services.alert;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceCrud;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/** Operations class for alerts. */
public class AlertLogOperations extends DatabaseOperations {

    private AlertLogCrud alertLogCrud = new AlertLogCrud();
    private PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();

    public void persist(AlertLog alert) {
        performTransaction(() -> alertLogCrud().persist(alert));
    }

    public List<AlertLog> alertLogs(PipelineInstance pipelineInstance) {
        return performTransaction(
            () -> alertLogCrud().retrieveForPipelineInstance(pipelineInstance));
    }

    public List<AlertLog> alertLogs(long pipelineInstanceId) {
        return performTransaction(() -> alertLogCrud()
            .retrieveForPipelineInstance(pipelineInstanceCrud().retrieve(pipelineInstanceId)));
    }

    public List<AlertLog> alertLogs(List<PipelineTask> tasks) {
        return performTransaction(() -> alertLogCrud().retrieveByPipelineTasks(tasks));
    }

    AlertLogCrud alertLogCrud() {
        return alertLogCrud;
    }

    PipelineInstanceCrud pipelineInstanceCrud() {
        return pipelineInstanceCrud;
    }
}
