package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;

/**
 * CRUD class for {@link FailedImport} instances.
 *
 * @author PT
 */
public class FailedImportCrud extends AbstractCrud<FailedImport> {

    /**
     * Creates a collection of new {@link FailedImport} rows in the database.
     */
    public void create(PipelineTask pipelineTask, Collection<Path> filenames,
        DatastoreProducerConsumer.DataReceiptFileType type) {

        for (Path filename : filenames) {
            persist(new FailedImport(pipelineTask, filename, type));
        }
    }

    /**
     * Retrieves all failed imports for a given pipeline instance.
     */
    public List<FailedImport> retrieveForInstance(long pipelineInstanceId) {

        // Start with task IDs
        List<PipelineTask> tasks = new PipelineTaskCrud().retrieveTasksForModuleAndInstance(
            DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME, pipelineInstanceId);
        Set<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toSet());
        ZiggyQuery<FailedImport, FailedImport> query = createZiggyQuery(FailedImport.class);
        query.column(FailedImport_.dataReceiptTaskId).in(taskIds).distinct(true);
        return list(query);
    }

    public int retrieveCountForInstance(long pipelineInstanceId) {
        return retrieveForInstance(pipelineInstanceId).size();
    }

    /**
     * For testing use only.
     *
     * @return All contents of the failed import table.
     */
    public List<FailedImport> retrieveAll() {
        return list(createZiggyQuery(FailedImport.class));
    }

    @Override
    public Class<FailedImport> componentClass() {
        return FailedImport.class;
    }
}
