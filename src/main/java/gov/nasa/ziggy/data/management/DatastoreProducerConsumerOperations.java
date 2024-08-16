package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for methods primarily concerned with {@link DatastoreProducerConsumer}
 * instances.
 *
 * @author PT
 */
public class DatastoreProducerConsumerOperations extends DatabaseOperations {

    private DatastoreProducerConsumerCrud datastoreProducerConsumerCrud = new DatastoreProducerConsumerCrud();
    private PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

    public Set<String> filesConsumedByTasks(Collection<Long> consumerIds,
        Collection<String> filenames) {
        return performTransaction(() -> datastoreProducerConsumerCrud()
            .retrieveFilesConsumedByTasks(consumerIds, filenames));
    }

    public void createOrUpdateProducer(PipelineTask pipelineTask, Collection<Path> files) {
        performTransaction(
            () -> datastoreProducerConsumerCrud().createOrUpdateProducer(pipelineTask, files));
    }

    public void addConsumer(PipelineTask pipelineTask, Set<String> datastoreNames) {
        performTransaction(
            () -> datastoreProducerConsumerCrud().addConsumer(pipelineTask, datastoreNames));
    }

    public void addNonProducingConsumer(PipelineTask pipelineTask, Set<String> datastoreNames) {
        performTransaction(() -> datastoreProducerConsumerCrud()
            .addNonProducingConsumer(pipelineTask, datastoreNames));
    }

    public Set<Long> producerIds(PipelineTask pipelineTask) {
        return performTransaction(
            () -> new HashSet<>(datastoreProducerConsumerCrud().retrieveProducerIds(pipelineTask)));
    }

    public List<String> consumedFiles(Set<Path> producedFiles) {
        return performTransaction(
            () -> datastoreProducerConsumerCrud().retrieveConsumedFiles(producedFiles));
    }

    public Set<Path> newFiles(Collection<Path> allFiles) {
        return performTransaction(() -> datastoreProducerConsumerCrud().newFiles(allFiles));
    }

    DatastoreProducerConsumerCrud datastoreProducerConsumerCrud() {
        return datastoreProducerConsumerCrud;
    }

    PipelineTaskCrud pipelineTaskCrud() {
        return pipelineTaskCrud;
    }
}
