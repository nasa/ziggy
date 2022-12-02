package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.Criteria;
import org.hibernate.criterion.CriteriaSpecification;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumer.DataReceiptFileType;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * CRUD class for {@link DatastoreProducerConsumer}.
 * <p>
 * Note that while the {@link DatastoreProducerConsumer} primarily tracks mission data files, it is
 * also used to track the tasks that performed imports of instrument models. The model files do not
 * get their consumers tracked by the {@link DatastoreProducerConsumer}, instead there is a
 * {@link ModelRegistry} of the current versions of all models that is provided to a
 * {@link PipelineInstance} when the instance is created, and which can be exposed by the instance
 * report. This is why the majority of all retrieval methods retrieve only data files and not model
 * files: for most CRUD operations, we are concerned with the data files. Model files are only
 * accessed when they are imported (at which time a {@link DatastoreProducerConsumer} record is
 * created for them), or when the data receipt displays are updated (at which time the
 * {@link #retrieveForInstance(long)} and {@link #retrieveCountForInstance(long)} methods are used.
 *
 * @author PT
 */
public class DatastoreProducerConsumerCrud extends AbstractCrud {

    public DatastoreProducerConsumerCrud() {
        super();
    }

    public DatastoreProducerConsumerCrud(DatabaseService dbService) {
        super(dbService);
    }

    /**
     * Create or update a producer record for a single file.
     *
     * @param pipelineTask
     * @param datastoreFile
     */
    public void createOrUpdateProducer(PipelineTask pipelineTask, Path datastoreFile,
        DataReceiptFileType type) {
        Set<Path> datastoreFileSet = new HashSet<>();
        datastoreFileSet.add(datastoreFile);
        createOrUpdateProducer(pipelineTask, datastoreFileSet, type);
    }

    /**
     * Create or update a set of files with the their PipelineTask ID as producer.
     *
     * @param datastoreFiles
     * @param pipelineTask
     */
    public void createOrUpdateProducer(PipelineTask pipelineTask, Collection<Path> datastoreFiles,
        DataReceiptFileType type) {
        if (datastoreFiles == null || datastoreFiles.isEmpty()) {
            return;
        }
        List<DatastoreProducerConsumer> datastoreProducerConsumers = retrieveOrCreate(pipelineTask,
            datastoreNames(datastoreFiles), type);
        for (DatastoreProducerConsumer datastoreProducerConsumer : datastoreProducerConsumers) {
            datastoreProducerConsumer.setProducer(pipelineTask.getId());
            super.createOrUpdate(datastoreProducerConsumer);
        }
    }

    public List<DatastoreProducerConsumer> retrieveByFilename(Set<Path> datastoreFiles) {
        return retrieveOrCreate(null, datastoreNames(datastoreFiles), DataReceiptFileType.DATA);
    }

    /**
     * Retrieves the set of names of datastore files consumed by a specified pipeline task.
     */
    public Set<String> retrieveFilesConsumedByTask(long taskId) {

        Criteria criteria = createCriteria(DatastoreProducerConsumer.class);
        criteria.setProjection(Projections.property("filename"));
        criteria.createAlias("consumers", "consumers");
        criteria.add(Restrictions.in("consumers.elements", Collections.singleton(taskId)));
        return new HashSet<>(list(criteria));
    }

    /**
     * Retrieve producers for a set of files.
     */
    public Set<Long> retrieveProducers(Set<Path> datastoreFiles) {
        if (datastoreFiles == null || datastoreFiles.isEmpty()) {
            return new HashSet<>();
        }
        Set<String> datastoreNames = datastoreNames(datastoreFiles);
        List<DatastoreProducerConsumer> dpcs = retrieveOrCreate(null, datastoreNames,
            DataReceiptFileType.DATA);
        return dpcs.stream()
            .map(DatastoreProducerConsumer::getProducer)
            .collect(Collectors.toSet());
    }

    /**
     * Adds a consumer to each of a set of datastore files.
     */
    public void addConsumer(PipelineTask pipelineTask, Set<String> datastoreNames) {
        if (datastoreNames == null || datastoreNames.isEmpty()) {
            return;
        }
        List<DatastoreProducerConsumer> dpcs = retrieveOrCreate(null, datastoreNames,
            DataReceiptFileType.DATA);
        dpcs.stream().forEach(s -> addConsumer(s, pipelineTask.getId()));
    }

    /**
     * Adds a non-producing consumer to each of a set of datastore files. A non-producing consumer
     * is a consumer that failed to produce results from processing. It is indicated by the negative
     * of the task ID.
     */
    public void addNonProducingConsumer(PipelineTask pipelineTask, Set<String> datastoreNames) {
        if (datastoreNames == null || datastoreNames.isEmpty()) {
            return;
        }
        List<DatastoreProducerConsumer> datastoreProducerConsumers = retrieveOrCreate(null,
            datastoreNames, DataReceiptFileType.DATA);
        datastoreProducerConsumers.stream().forEach(dpc -> addConsumer(dpc, -pipelineTask.getId()));
    }

    /**
     * Adds a consumer ID and updates or creates a {@link DatastoreProducerConsumer} instance.
     * Implemented as a private method so that a stream forEach operation can apply it.
     */
    private void addConsumer(DatastoreProducerConsumer datastoreProducerConsumer,
        long pipelineTaskId) {
        datastoreProducerConsumer.addConsumer(pipelineTaskId);
        super.createOrUpdate(datastoreProducerConsumer);
    }

    private Set<String> datastoreNames(Collection<Path> datastoreFiles) {
        return datastoreFiles.stream().map(Path::toString).collect(Collectors.toSet());
    }

    /**
     * Retrieves all {@link DatastoreProducerConsumer} database entries that correspond to a list of
     * filenames. For any filenames that lack such entries, instances of
     * {@link DatastoreProducerConsumer} will be constructed.
     *
     * @param pipelineTask Producer task for constructed entries, if null a value of zero will be
     * used for the producer ID of constructed entries
     * @param filenames Names of datastore files to be located in the database table of
     * {@link DatastoreProducerConsumer} instances
     * @return A {@link List} of {@link DatastoreProducerConsumer} instances, with the database
     * versions for files that have database entries and new instances for those that do not.
     */
    private List<DatastoreProducerConsumer> retrieveOrCreate(PipelineTask pipelineTask,
        Set<String> filenames, DataReceiptFileType type) {

        // Start by finding all the files that already have entries
        Criteria q = createCriteria(DatastoreProducerConsumer.class);
        q.add(restrictionPropertyIn("filename", filenames));
        List<DatastoreProducerConsumer> datastoreProducerConsumers = list(q);
        List<String> locatedFilenames = datastoreProducerConsumers.stream()
            .map(DatastoreProducerConsumer::getFilename)
            .collect(Collectors.toList());

        // For all the filenames that lack entries, construct DatastoreProducerConsumer instances
        long producerId = pipelineTask != null ? pipelineTask.getId() : 0;
        filenames.removeAll(locatedFilenames);
        for (String filename : filenames) {
            datastoreProducerConsumers
                .add(new DatastoreProducerConsumer(producerId, filename, type));
        }

        // Now put the non-found filenames back into the filenames argument, just in case
        // the user wants to use them for some other purpose
        filenames.addAll(locatedFilenames);

        return datastoreProducerConsumers;
    }

    /**
     * Retrieves all successful imports for a given pipeline instance.
     */
    public List<DatastoreProducerConsumer> retrieveForInstance(long pipelineInstanceId) {

        // Start with task IDs
        List<PipelineTask> tasks = new PipelineTaskCrud().retrieveTasksForModuleAndInstance(
            DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME, pipelineInstanceId);
        Set<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toSet());
        Criteria query = createCriteria(DatastoreProducerConsumer.class);
        query.add(Restrictions.in("producerId", taskIds));
        query.setResultTransformer(CriteriaSpecification.DISTINCT_ROOT_ENTITY);
        return list(query);
    }

    /**
     * Retrieves a count of successful imports for a given pipeline instance.
     */
    public int retrieveCountForInstance(long pipelineInstanceId) {
        return retrieveForInstance(pipelineInstanceId).size();
    }

    /**
     * Retrieve all the objects in the database.
     *
     * @return
     */
    public List<DatastoreProducerConsumer> retrieveAll() {
        Criteria q = createCriteria(DatastoreProducerConsumer.class);
        @SuppressWarnings("unchecked")
        List<DatastoreProducerConsumer> r = q.list();
        return r;
    }
}
