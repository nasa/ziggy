package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ModelRegistry;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseService;
import jakarta.persistence.criteria.Predicate;

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
public class DatastoreProducerConsumerCrud extends AbstractCrud<DatastoreProducerConsumer> {

    public DatastoreProducerConsumerCrud() {
    }

    public DatastoreProducerConsumerCrud(DatabaseService dbService) {
        super(dbService);
    }

    /** Create or update a producer record for a single file. */
    public void createOrUpdateProducer(PipelineTask pipelineTask, Path datastoreFile) {
        Set<Path> datastoreFileSet = new HashSet<>();
        datastoreFileSet.add(datastoreFile);
        createOrUpdateProducer(pipelineTask, datastoreFileSet);
    }

    /** Create or update a set of files with the their PipelineTask ID as producer. */
    public void createOrUpdateProducer(PipelineTask pipelineTask, Collection<Path> datastoreFiles) {
        if (datastoreFiles == null || datastoreFiles.isEmpty()) {
            return;
        }
        List<DatastoreProducerConsumer> datastoreProducerConsumers = retrieveOrCreate(pipelineTask,
            datastoreNames(datastoreFiles));
        for (DatastoreProducerConsumer datastoreProducerConsumer : datastoreProducerConsumers) {
            datastoreProducerConsumer.setProducer(pipelineTask);
            merge(datastoreProducerConsumer);
        }
    }

    /** Retrieves / creates {@link DatastoreProducerConsumer}s for a collection of files. */
    public List<DatastoreProducerConsumer> retrieveByFilename(Set<Path> datastoreFiles) {
        return retrieveOrCreate(null, datastoreNames(datastoreFiles));
    }

    /** Retrieves the set of names of datastore files consumed by a specified pipeline task. */
    public Set<String> retrieveFilesConsumedByTask(PipelineTask pipelineTask) {
        return retrieveFilesConsumedByTasks(Set.of(pipelineTask), null);
    }

    /**
     * Retrieves the set of filenames of datastore files that were consumed by one or more of the
     * specified consumer task IDs. If the filenames argument is populated, only files from the
     * filenames collection will be included in the return; otherwise, all filenames that have a
     * consumer from the collection of consumer IDs will be included.
     */
    public Set<String> retrieveFilesConsumedByTasks(Collection<PipelineTask> consumers,
        Collection<String> filenames) {

        if (CollectionUtils.isEmpty(filenames)) {
            return new HashSet<>(list(filesConsumedByTasksQuery(consumers)));
        }
        return new HashSet<>(chunkedQuery(new ArrayList<>(filenames),
            chunk -> list(
                filesConsumedByTasksQuery(consumers).column(DatastoreProducerConsumer_.filename)
                    .in(chunk))));
    }

    private ZiggyQuery<DatastoreProducerConsumer, String> filesConsumedByTasksQuery(
        Collection<PipelineTask> consumers) {

        ZiggyQuery<DatastoreProducerConsumer, String> query = createZiggyQuery(
            DatastoreProducerConsumer.class, String.class);
        query.select(DatastoreProducerConsumer_.filename).distinct(true);
        addConsumerIdPredicates(query,
            consumers.stream().map(PipelineTask::getId).collect(Collectors.toList()));
        return query;
    }

    /**
     * Adds predicates that finds {@link DatastoreProducerConsumer} instances that contain any of a
     * collection of consumer IDs.
     */
    private void addConsumerIdPredicates(ZiggyQuery<DatastoreProducerConsumer, ?> query,
        Collection<Long> consumerIds) {
        List<Predicate> predicates = new ArrayList<>();
        for (long consumerId : consumerIds) {
            predicates.add(query.getBuilder()
                .isMember(consumerId, query.getRoot().get(DatastoreProducerConsumer_.consumers)));
        }
        Predicate[] predicateArray = predicates.toArray(new Predicate[0]);
        for (int predicateIndex = 0; predicateIndex < predicates.size(); predicateIndex++) {
            predicateArray[predicateIndex] = predicates.get(predicateIndex);
        }
        Predicate completePredicate = query.getBuilder().or(predicateArray);
        query.where(completePredicate);
    }

    /** Retrieve producers for a set of files. */
    public Set<Long> retrieveProducers(Set<Path> datastoreFiles) {
        if (datastoreFiles == null || datastoreFiles.isEmpty()) {
            return new HashSet<>();
        }
        Set<String> datastoreNames = datastoreNames(datastoreFiles);
        List<DatastoreProducerConsumer> dpcs = retrieveOrCreate(null, datastoreNames);
        return dpcs.stream()
            .map(DatastoreProducerConsumer::getProducer)
            .collect(Collectors.toSet());
    }

    /**
     * Retrieve consumed files for a collection of produced files. That is, for a given set of files
     * that are outputs from processing, find the files that were used as inputs to produce the
     * specified outputs.
     */
    public List<String> retrieveConsumedFiles(Set<Path> producedFiles) {
        if (CollectionUtils.isEmpty(producedFiles)) {
            return new ArrayList<>();
        }

        // Convert any absolute paths to be relative to the datastore root.
        Set<Path> producedFilesRelativePaths = new HashSet<>();
        for (Path path : producedFiles) {
            producedFilesRelativePaths.add(path.isAbsolute()
                ? DirectoryProperties.datastoreRootDir().toAbsolutePath().relativize(path)
                : path);
        }

        // Find the datastore names for the output files.
        Set<String> producedFileDatastoreNames = datastoreNames(producedFilesRelativePaths);

        // Find the unique set of pipeline tasks that produced the output files.
        List<Long> producerIds = chunkedQuery(new ArrayList<>(producedFileDatastoreNames),
            chunk -> list(createZiggyQuery(DatastoreProducerConsumer.class, Long.class)
                .column(DatastoreProducerConsumer_.filename)
                .in(chunk)
                .column(DatastoreProducerConsumer_.producerId)
                .select()
                .distinct(true)));

        // Find and return the files that were consumed by the tasks that produced the
        // outputs.
        ZiggyQuery<DatastoreProducerConsumer, String> query = createZiggyQuery(
            DatastoreProducerConsumer.class, String.class)
                .column(DatastoreProducerConsumer_.filename)
                .select();
        addConsumerIdPredicates(query, producerIds);
        return list(query);
    }

    /** Adds a consumer to each of a set of datastore files. */
    public void addConsumer(PipelineTask pipelineTask, Set<String> datastoreNames) {
        if (datastoreNames == null || datastoreNames.isEmpty()) {
            return;
        }
        List<DatastoreProducerConsumer> dpcs = retrieveOrCreate(null, datastoreNames);
        dpcs.stream().forEach(s -> addConsumer(s, pipelineTask));
    }

    /**
     * Adds a non-producing consumer to each of a set of datastore files. A non-producing consumer
     * is a consumer that failed to produce results from processing.
     */
    public void addNonProducingConsumer(PipelineTask pipelineTask, Set<String> datastoreNames) {
        if (datastoreNames == null || datastoreNames.isEmpty()) {
            return;
        }
        List<DatastoreProducerConsumer> datastoreProducerConsumers = retrieveOrCreate(null,
            datastoreNames);
        datastoreProducerConsumers.stream()
            .forEach(dpc -> addNonProducingConsumer(dpc, pipelineTask));
    }

    /**
     * Adds a consumer to the given {@link DatastoreProducerConsumer} instance.
     */
    private void addConsumer(DatastoreProducerConsumer datastoreProducerConsumer,
        PipelineTask pipelineTask) {
        datastoreProducerConsumer.addConsumer(pipelineTask);
        merge(datastoreProducerConsumer);
    }

    /**
     * Adds a consumer to the given {@link DatastoreProducerConsumer} instance.
     */
    private void addNonProducingConsumer(DatastoreProducerConsumer datastoreProducerConsumer,
        PipelineTask pipelineTask) {
        datastoreProducerConsumer.addNonProducingConsumer(pipelineTask);
        merge(datastoreProducerConsumer);
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
     * {@link DatastoreProducerConsumer} instances.
     * @return A {@link List} of {@link DatastoreProducerConsumer} instances, with the database
     * versions for files that have database entries and new instances for those that do not
     */
    protected List<DatastoreProducerConsumer> retrieveOrCreate(PipelineTask pipelineTask,
        Set<String> filenames) {

        // Start by finding all the files that already have entries.
        Set<String> allFilenames = new HashSet<>(filenames);
        List<DatastoreProducerConsumer> datastoreProducerConsumers = chunkedQuery(
            new ArrayList<>(allFilenames),
            chunk -> list(createZiggyQuery(DatastoreProducerConsumer.class)
                .column(DatastoreProducerConsumer_.filename)
                .in(chunk)));

        List<String> locatedFilenames = datastoreProducerConsumers.stream()
            .map(DatastoreProducerConsumer::getFilename)
            .collect(Collectors.toList());

        // For all the filenames that lack entries, construct DatastoreProducerConsumer instances.
        allFilenames.removeAll(locatedFilenames);
        for (String filename : allFilenames) {
            DatastoreProducerConsumer instance = new DatastoreProducerConsumer(pipelineTask,
                filename);
            persist(instance);
            datastoreProducerConsumers.add(instance);
        }

        // Now put the non-found filenames back into the filenames argument, just in case
        // the user wants to use them for some other purpose
        allFilenames.addAll(locatedFilenames);

        return datastoreProducerConsumers;
    }

    /** Retrieves all successful imports for a given pipeline instance. */
    public List<DatastoreProducerConsumer> retrieveForInstance(long pipelineInstanceId) {

        // Start with task IDs
        List<PipelineTask> tasks = new PipelineTaskCrud().retrieveTasksForModuleAndInstance(
            DataReceiptPipelineModule.DATA_RECEIPT_MODULE_NAME, pipelineInstanceId);
        Set<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toSet());

        ZiggyQuery<DatastoreProducerConsumer, DatastoreProducerConsumer> query = createZiggyQuery(
            DatastoreProducerConsumer.class);
        query.column(DatastoreProducerConsumer_.producerId).in(taskIds).distinct(true);

        return list(query);
    }

    /** Retrieves a count of successful imports for a given pipeline instance. */
    public int retrieveCountForInstance(long pipelineInstanceId) {
        return retrieveForInstance(pipelineInstanceId).size();
    }

    /** Retrieve all the objects in the database. */
    public List<DatastoreProducerConsumer> retrieveAll() {
        return list(createZiggyQuery(DatastoreProducerConsumer.class));
    }

    public List<Long> retrieveProducerIds(PipelineTask pipelineTask) {
        ZiggyQuery<DatastoreProducerConsumer, Long> query = createZiggyQuery(
            DatastoreProducerConsumer.class, Long.class);
        query.column(DatastoreProducerConsumer_.consumers).contains(pipelineTask.getId());
        query.column(DatastoreProducerConsumer_.producerId).select().distinct(true);
        return list(query);
    }

    /**
     * Identify files that are new, i.e., files that do not exist in the producer-consumer table.
     */
    public Set<Path> newFiles(Collection<Path> datastoreFiles) {

        // We need a Map from the names to the datastore file Path objects.
        Map<String, Path> datastoreFileByName = datastoreFiles.stream()
            .collect(Collectors.toMap(Path::toString, Function.identity()));
        Set<String> datastoreNames = new HashSet<>(datastoreFileByName.keySet());

        datastoreNames.removeAll(chunkedQuery(new ArrayList<>(datastoreNames),
            chunk -> list(createZiggyQuery(DatastoreProducerConsumer.class, String.class)
                .column(DatastoreProducerConsumer_.filename)
                .in(chunk)
                .select())));
        return datastoreNames.stream().map(datastoreFileByName::get).collect(Collectors.toSet());
    }

    @Override
    public Class<DatastoreProducerConsumer> componentClass() {
        return DatastoreProducerConsumer.class;
    }
}
