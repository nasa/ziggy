package gov.nasa.ziggy.data.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Test class for {@link DatastoreProducerConsumerOperations}.
 *
 * @author PT
 */
public class DatastoreProducerConsumerOperationsTest {

    private DatastoreProducerConsumerOperations datastoreProducerConsumerOperations = new DatastoreProducerConsumerOperations();
    private TestOperations testOperations = new TestOperations();
    private PipelineTask pipelineTask;
    private static final long TASK_ID = 30L;
    private static final String FILE_NAME_1 = "d1/d2/d3/fake-file.h5";
    private static final String FILE_NAME_2 = "d1/d2/d3/fake-file-2.h5";
    private static final String FILE_NAME_3 = "d1/d2/d3/fake-file-3.h5";
    private static final Path PATH_1 = Paths.get(FILE_NAME_1);
    private static final Path PATH_2 = Paths.get(FILE_NAME_2);
    private static final Path PATH_3 = Paths.get(FILE_NAME_3);

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setup() {
        pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID);
    }

    /**
     * Tests both createOrUpdateOriginator() signatures.
     */
    @Test
    public void testCreateOrUpdateOriginator() {

        /**
         * First test the version that takes a single ResultsOriginator object
         */
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_1));
        List<DatastoreProducerConsumer> r0 = testOperations.allProducerConsumerInstances();

        assertEquals(1, r0.size());
        DatastoreProducerConsumer r00 = r0.get(0);
        assertEquals(TASK_ID, r00.getProducer());
        assertEquals(FILE_NAME_1, r00.getFilename());

        /**
         * Now test the version that takes a set of ResultsOriginator objects; while we are at it we
         * can test the "update" part of "create or update"
         */

        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID + 1);
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask,
            List.of(PATH_1, PATH_2, PATH_3));

        r0 = testOperations.allProducerConsumerInstances();

        assertEquals(3, r0.size());
        Map<String, Long> resultsOriginatorMap = new HashMap<>();
        for (DatastoreProducerConsumer r : r0) {
            resultsOriginatorMap.put(r.getFilename(), r.getProducer());
        }
        Set<String> filenames = resultsOriginatorMap.keySet();
        assertTrue(filenames.contains(FILE_NAME_1));
        assertEquals(TASK_ID + 1, (long) resultsOriginatorMap.get(FILE_NAME_1));
        assertTrue(filenames.contains(FILE_NAME_2));
        assertEquals(TASK_ID + 1, (long) resultsOriginatorMap.get(FILE_NAME_2));
        assertTrue(filenames.contains(FILE_NAME_1));
        assertEquals(TASK_ID + 1, (long) resultsOriginatorMap.get(FILE_NAME_3));
    }

    /**
     * Tests the case in which the originators for all of the requested files are the same.
     */
    @Test
    public void retrieveOriginatorsAllSame() {
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_1));
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_2));
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_3));
        Set<Long> originators = testOperations.producers(Set.of(PATH_1, PATH_2));
        assertEquals(1, originators.size());
        assertTrue(originators.contains(TASK_ID));
    }

    /**
     * Tests the case in which the originators of the requested files are different.
     */
    @Test
    public void retrieveOriginatorsAllDifferent() {
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_1));
        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID + 1);
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_2));
        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID + 2);
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_3));
        Set<Long> originators = testOperations.producers(Set.of(PATH_1, PATH_2));
        assertEquals(2, originators.size());
        assertTrue(originators.contains(TASK_ID));
        assertTrue(originators.contains(TASK_ID + 1));
    }

    /**
     * Tests retrieval of files that are consumed by a specified task.
     */
    @Test
    public void testRetrieveFilesConsumedByTask() {

        // Create some files in the producer-consumer database table, and add consumers to them.
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_1));
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_2));
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_3));
        PipelineTask consumer1 = Mockito.mock(PipelineTask.class);
        Mockito.when(consumer1.getId()).thenReturn(31L);
        datastoreProducerConsumerOperations.addConsumer(consumer1,
            new HashSet<>(Set.of(PATH_1.toString(), PATH_3.toString())));
        PipelineTask consumer2 = Mockito.mock(PipelineTask.class);
        Mockito.when(consumer2.getId()).thenReturn(32L);
        datastoreProducerConsumerOperations.addConsumer(consumer2,
            new HashSet<>(Set.of(PATH_1.toString(), PATH_2.toString())));
        PipelineTask consumer3 = Mockito.mock(PipelineTask.class);
        Mockito.when(consumer3.getId()).thenReturn(33L);
        datastoreProducerConsumerOperations.addConsumer(consumer3,
            new HashSet<>(Set.of(PATH_2.toString(), PATH_3.toString())));

        // Retrieve the files that have the relevant pipeline task as consumer.
        Set<String> files = testOperations.filesConsumedByTask(consumer1);
        assertEquals(2, files.size());
        assertTrue(files.contains(PATH_1.toString()));
        assertTrue(files.contains(PATH_3.toString()));
    }

    @Test
    public void testRetrieveFilesConsumedByTasks() {

        // Put the files into the database.
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask,
            List.of(PATH_1, PATH_2, PATH_3));

        // Add consumers.
        PipelineTask pipelineTask100 = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask100.getId()).thenReturn(100L);
        datastoreProducerConsumerOperations.addConsumer(pipelineTask100, Set.of(PATH_1.toString()));
        PipelineTask pipelineTask110 = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask110.getId()).thenReturn(110L);
        datastoreProducerConsumerOperations.addConsumer(pipelineTask110, Set.of(PATH_3.toString()));
        PipelineTask pipelineTask120 = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask120.getId()).thenReturn(120L);
        datastoreProducerConsumerOperations.addConsumer(pipelineTask120, Set.of(PATH_2.toString()));

        PipelineTask pipelineTask105 = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask105.getId()).thenReturn(105L);

        Set<String> filenames = testOperations
            .filesConsumedByTasks(Set.of(pipelineTask100, pipelineTask105), null);
        assertTrue(filenames.contains(PATH_1.toString()));
        assertEquals(1, filenames.size());
        filenames = testOperations
            .filesConsumedByTasks(Set.of(pipelineTask100, pipelineTask105, pipelineTask110), null);
        assertTrue(filenames.contains(PATH_1.toString()));
        assertTrue(filenames.contains(PATH_3.toString()));
        assertEquals(2, filenames.size());
        filenames = testOperations.filesConsumedByTasks(
            Set.of(pipelineTask100, pipelineTask105, pipelineTask110),
            Set.of(PATH_2.toString(), PATH_3.toString()));
        assertTrue(filenames.contains(PATH_3.toString()));
        assertEquals(1, filenames.size());
    }

    @Test
    public void testConsumedFiles() {

        // Put two files into the system as produced by the sample pipeline task.
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask,
            List.of(PATH_1, PATH_2));

        // Create a consumer of the two files.
        Mockito.when(pipelineTask.getId()).thenReturn(100L);
        datastoreProducerConsumerOperations.addConsumer(pipelineTask,
            Set.of(PATH_1.toString(), PATH_2.toString()));

        // Create a file produced by the new task.
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask, List.of(PATH_3));
        List<String> consumedFiles = datastoreProducerConsumerOperations
            .consumedFiles(Set.of(PATH_3));
        assertTrue(consumedFiles.contains(PATH_1.toString()));
        assertTrue(consumedFiles.contains(PATH_2.toString()));
        assertEquals(2, consumedFiles.size());
    }

    @Test
    public void testNewFiles() {

        // Put two files into the system as produced by the sample pipeline task.
        datastoreProducerConsumerOperations.createOrUpdateProducer(pipelineTask,
            List.of(PATH_1, PATH_2));

        // Find the new files out of a set of all 3 PATH_ files.
        Set<Path> newFiles = datastoreProducerConsumerOperations
            .newFiles(List.of(PATH_1, PATH_2, PATH_3));
        assertTrue(newFiles.contains(PATH_3));
        assertEquals(1, newFiles.size());
    }

    private static class TestOperations extends DatabaseOperations {

        public List<DatastoreProducerConsumer> allProducerConsumerInstances() {
            return performTransaction(() -> new DatastoreProducerConsumerCrud().retrieveAll());
        }

        public Set<Long> producers(Set<Path> files) {
            return performTransaction(
                () -> new DatastoreProducerConsumerCrud().retrieveProducers(files));
        }

        public Set<String> filesConsumedByTask(PipelineTask pipelineTask) {
            return performTransaction(() -> new DatastoreProducerConsumerCrud()
                .retrieveFilesConsumedByTask(pipelineTask));
        }

        public Set<String> filesConsumedByTasks(Set<PipelineTask> pipelineTasks,
            Collection<String> filenames) {
            return performTransaction(() -> new DatastoreProducerConsumerCrud()
                .retrieveFilesConsumedByTasks(pipelineTasks, filenames));
        }
    }
}
