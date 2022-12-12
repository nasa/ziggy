package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumer;
import gov.nasa.ziggy.data.management.DatastoreProducerConsumerCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Test class for {@link DatastoreProducerConsumerCrud}.
 *
 * @author PT
 */
public class DatastoreProducerConsumerCrudTest {

    private DatastoreProducerConsumerCrud resultsOriginatorCrud;
    private PipelineTask pipelineTask;
    private static final long TASK_ID = 30L;
    private static final String FILE_NAME_1 = "d1/d2/d3/fake-file.h5";
    private static final String FILE_NAME_2 = "d1/d2/d3/fake-file-2.h5";
    private static final String FILE_NAME_3 = "d1/d2/d3/fake-file-3.h5";
    private static final Path PATH_1 = Paths.get(FILE_NAME_1);
    private static final Path PATH_2 = Paths.get(FILE_NAME_2);
    private static final Path PATH_3 = Paths.get(FILE_NAME_3);

    @Before
    public void setup() {
        ZiggyUnitTestUtils.setUpDatabase();
        resultsOriginatorCrud = new DatastoreProducerConsumerCrud();
        pipelineTask = Mockito.mock(PipelineTask.class);
        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID);

    }

    @After
    public void teardown() {
        ZiggyUnitTestUtils.tearDownDatabase();
    }

    /**
     * Tests both createOrUpdateOriginator() signatures.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCreateOrUpdateOriginator() {

        /**
         * First test the version that takes a single ResultsOriginator object
         */
        DatabaseTransactionFactory.performTransaction(() -> {
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_1,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            return null;
        });
        List<DatastoreProducerConsumer> r0 = (List<DatastoreProducerConsumer>) DatabaseTransactionFactory
            .performTransaction(() -> resultsOriginatorCrud.retrieveAll());

        assertEquals(1, r0.size());
        DatastoreProducerConsumer r00 = r0.get(0);
        assertEquals(TASK_ID, r00.getProducer());
        assertEquals(FILE_NAME_1, r00.getFilename());

        /**
         * Now test the version that takes a set of ResultsOriginator objects; while we are at it we
         * can test the "update" part of "create or update"
         */

        Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID + 1);
        DatabaseTransactionFactory.performTransaction(() -> {
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask,
                Sets.newHashSet(PATH_1, PATH_2, PATH_3),
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            return null;
        });

        r0 = (List<DatastoreProducerConsumer>) DatabaseTransactionFactory
            .performTransaction(() -> resultsOriginatorCrud.retrieveAll());
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
        DatabaseTransactionFactory.performTransaction(() -> {
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_1,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_2,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_3,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            return null;
        });
        @SuppressWarnings("unchecked")
        Set<Long> originators = (Set<Long>) DatabaseTransactionFactory.performTransaction(
            () -> resultsOriginatorCrud.retrieveProducers(Sets.newHashSet(PATH_1, PATH_2)));
        assertEquals(1, originators.size());
        assertTrue(originators.contains(TASK_ID));
    }

    /**
     * Tests the case in which the originators of the requested files are different.
     */
    @Test
    public void retrieveOriginatorsAllDifferent() {
        DatabaseTransactionFactory.performTransaction(() -> {
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_1,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID + 1);
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_2,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            Mockito.when(pipelineTask.getId()).thenReturn(TASK_ID + 2);
            resultsOriginatorCrud.createOrUpdateProducer(pipelineTask, PATH_3,
                DatastoreProducerConsumer.DataReceiptFileType.DATA);
            return null;
        });
        @SuppressWarnings("unchecked")
        Set<Long> originators = (Set<Long>) DatabaseTransactionFactory.performTransaction(
            () -> resultsOriginatorCrud.retrieveProducers(Sets.newHashSet(PATH_1, PATH_2)));
        assertEquals(2, originators.size());
        assertTrue(originators.contains(TASK_ID));
        assertTrue(originators.contains(TASK_ID + 1));
    }

}
