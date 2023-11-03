package gov.nasa.ziggy.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.util.StringUtils;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Unit tests for {@link ZiggyQuery}.
 *
 * @author PT
 */
public class ZiggyQueryTest {

    private final String HIBERNATE_LOG_FILE_NAME = "hibernate.log";
    private final String APPENDER_NAME = "file-appender.log";
    private final String TABLE_NAME = "ziggy_PipelineTask";
    private final String DUMMY_TASK_NAME = "p1_0";

    private PipelineTaskCrud crud;
    private Path logPath;
    private FileAppender hibernateLog;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule showSqlProperty = new ZiggyPropertyRule(
        PropertyName.HIBERNATE_SHOW_SQL, "true");

    @Rule
    public ZiggyPropertyRule formatSqlProperty = new ZiggyPropertyRule(
        PropertyName.HIBERNATE_FORMAT_SQL, "true");

    @Rule
    public ZiggyPropertyRule sqlCommentsProperty = new ZiggyPropertyRule(
        PropertyName.HIBERNATE_USE_SQL_COMMENTS, "true");

    @Rule
    public ZiggyPropertyRule log4jConfigProperty = new ZiggyPropertyRule(
        PropertyName.LOG4J2_CONFIGURATION_FILE, Paths.get("etc").resolve("log4j2.xml").toString());

    public ZiggyPropertyRule log4jLogFileProperty = new ZiggyPropertyRule("ziggy.logfile",
        directoryRule, HIBERNATE_LOG_FILE_NAME);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(log4jLogFileProperty);

    @Before
    public void setUp() {
        crud = new PipelineTaskCrud();

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration c = ctx.getConfiguration();
        LoggerConfig lc = c.getLoggerConfig("");
        lc.setLevel(Level.ALL);

        logPath = directoryRule.directory().resolve(HIBERNATE_LOG_FILE_NAME);

        hibernateLog = FileAppender.newBuilder()
            .withFileName(logPath.toString())
            .setName(APPENDER_NAME)
            .build();
        hibernateLog.start();
        lc.addAppender(hibernateLog, Level.ALL, null);
    }

    @After
    public void shutDown() {
        LoggerConfig rootConfig = ((LoggerContext) LogManager.getContext(false)).getConfiguration()
            .getLoggerConfig("");
        Appender appender = rootConfig.getAppenders().get(APPENDER_NAME);
        if (appender != null) {
            appender.stop();
            rootConfig.removeAppender(APPENDER_NAME);
        }
    }

    /**
     * Tests that the SQL for a simple query is as expected.
     */
    @Test
    public void sqlRetrieveTest() throws IOException {

        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertTrue(tasks.isEmpty());

        // Read in the log file
        List<String> logFileContents = logFileContents();
        assertFalse(CollectionUtils.isEmpty(logFileContents));

        // The last line should hold the SQL for the query.
        String queryString = logFileContents.get(logFileContents.size() - 1);
        int queryStart = queryString.indexOf("select");
        assertTrue(queryStart > 0);

        // The name of the table and the name of the dummy retrieved task should be at the end.
        int tableNameStart = queryString.indexOf("from " + TABLE_NAME + " " + DUMMY_TASK_NAME);
        assertTrue(tableNameStart > queryStart);

        // Get the fields for a PipelineTask, removing any fields that are lazy initialized, as they
        // don't appear in the query.
        Field[] fields = PipelineTask.class.getDeclaredFields();
        List<String> fieldNames = new ArrayList<>();
        Set<String> lazyFieldNames = Set.of("log", "uowTaskParameters", "summaryMetrics", "execLog",
            "producerTaskIds", "remoteJobs");

        for (Field field : fields) {
            if (!lazyFieldNames.contains(field.getName())) {
                fieldNames.add(field.getName());
            }
        }

        // All the non-lazy fields should appear between the select statement and the from
        // statement.
        for (String fieldName : fieldNames) {
            String fullFieldName = DUMMY_TASK_NAME + "." + fieldName;
            assertTrue(fieldName, queryString.indexOf(fullFieldName) > queryStart
                && queryString.indexOf(fullFieldName) < tableNameStart);
        }

        // None of the lazy fields should appear at all.
        for (String fieldName : lazyFieldNames) {
            String fullFieldName = DUMMY_TASK_NAME + "." + fieldName;
            assertEquals(fieldName, -1, queryString.indexOf(fullFieldName));
        }
    }

    /**
     * Tests that a simple query that retrieves all instances behaves as expected.
     */
    @Test
    public void testRetrieveAllInstances() {

        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, tasks.size());
    }

    /**
     * Tests that a single-valued column criterion behaves as expected.
     */
    @Test
    public void testColumnCriterion() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.state).in(PipelineTask.State.COMPLETED);
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(1L));
        assertTrue(taskIds.contains(4L));
    }

    /**
     * Tests that the {@link ZiggyQuery#where(jakarta.persistence.criteria.Predicate)} method
     * accepts a user-constructed predicate.
     */
    @Test
    public void testWhereClause() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.where(query.getBuilder().between(query.getRoot().get(PipelineTask_.id), 2L, 3L));
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(2L));
        assertTrue(taskIds.contains(3L));
    }

    /**
     * Tests the {@link ZiggyQuery#get(String)} and
     * {@link ZiggyQuery#in(jakarta.persistence.criteria.Expression, java.util.Collection)} methods.
     */
    @Test
    public void testGetInClauses() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.where(query.in(query.get(PipelineTask_.id), Set.of(1L, 2L)));
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(1L));
        assertTrue(taskIds.contains(2L));
    }

    /**
     * Tests the {@link ZiggyQuery#in(jakarta.persistence.criteria.Expression, Object)} method.
     */
    @Test
    public void testScalarInClause() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.where(query.in(query.get(PipelineTask_.id), 1L));
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(1, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(1L));
    }

    /**
     * Tests that a multi-valued column criterion behaves as expected (i.e., any task that matches
     * any of the criteria is returned).
     */
    @Test
    public void testColumnMultiValueCriterion() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.state)
            .in(Set.of(PipelineTask.State.COMPLETED, PipelineTask.State.PROCESSING));
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(3, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(1L));
        assertTrue(taskIds.contains(2L));
        assertTrue(taskIds.contains(4L));
    }

    /**
     * Test that multiple criteria on a single column are combined by an AND operation.
     */
    @Test
    public void testColumnMultipleCriteria() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.id).in(Set.of(2L, 4L)).between(1L, 3L);
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(1, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(2L));
    }

    /**
     * Tests that criteria on multiple columns are combined by an AND operation.
     */
    @Test
    public void testCriteriaOnMultipleColumns() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.id)
            .in(Set.of(2L, 3L, 4L))
            .column(PipelineTask_.state)
            .in(PipelineTask.State.COMPLETED);
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(1, tasks.size());
        List<Long> taskIds = tasks.stream().map(PipelineTask::getId).collect(Collectors.toList());
        assertTrue(taskIds.contains(4L));
    }

    /**
     * Tests that a descending sort works as expected.
     */
    @Test
    public void testSort() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.id).descendingOrder();
        @SuppressWarnings("unchecked")
        List<PipelineTask> tasks = (List<PipelineTask>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, tasks.size());
        assertEquals(Long.valueOf(4L), tasks.get(0).getId());
        assertEquals(Long.valueOf(3L), tasks.get(1).getId());
        assertEquals(Long.valueOf(2L), tasks.get(2).getId());
        assertEquals(Long.valueOf(1L), tasks.get(3).getId());
    }

    /**
     * Tests that a single-column select operation behaves as expected.
     */
    @Test
    public void testSelectColumn() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.column(PipelineTask_.id).select();
        @SuppressWarnings("unchecked")
        List<Long> ids = (List<Long>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, ids.size());
        assertTrue(ids.contains(1L));
        assertTrue(ids.contains(2L));
        assertTrue(ids.contains(3L));
        assertTrue(ids.contains(4L));
    }

    /**
     * Tests that a single-column select operation where the column name is supplied as an argument
     * to the select method behaves as expected.
     */
    @Test
    public void testSelectColumnWithColNameArg() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.select(PipelineTask_.id);
        @SuppressWarnings("unchecked")
        List<Long> ids = (List<Long>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, ids.size());
        assertTrue(ids.contains(1L));
        assertTrue(ids.contains(2L));
        assertTrue(ids.contains(3L));
        assertTrue(ids.contains(4L));
    }

    /**
     * Tests that the {@link ZiggyQuery#select(jakarta.persistence.criteria.Selection)} method
     * accepts a user-constructed selection instance.
     */
    @Test
    public void testSelectClause() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.select(query.getRoot().get("id"));
        @SuppressWarnings("unchecked")
        List<Long> ids = (List<Long>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, ids.size());
        assertTrue(ids.contains(1L));
        assertTrue(ids.contains(2L));
        assertTrue(ids.contains(3L));
        assertTrue(ids.contains(4L));
    }

    /**
     * Tests that non-distinct selection works as expected.
     */
    @Test
    public void testNonDistinctSelection() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask.State> query = crud
            .createZiggyQuery(PipelineTask.class, PipelineTask.State.class);
        query.column(PipelineTask_.state).select();
        @SuppressWarnings("unchecked")
        List<PipelineTask.State> states = (List<PipelineTask.State>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, states.size());
        assertTrue(states.contains(PipelineTask.State.COMPLETED));
        assertTrue(states.contains(PipelineTask.State.PROCESSING));
        assertTrue(states.contains(PipelineTask.State.ERROR));
    }

    /**
     * Tests that distinct selection works as expected.
     */
    @Test
    public void testDistinctSelection() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask.State> query = crud
            .createZiggyQuery(PipelineTask.class, PipelineTask.State.class);
        query.column(PipelineTask_.state).select();
        query.distinct(true);
        @SuppressWarnings("unchecked")
        List<PipelineTask.State> states = (List<PipelineTask.State>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(3, states.size());
        assertTrue(states.contains(PipelineTask.State.COMPLETED));
        assertTrue(states.contains(PipelineTask.State.PROCESSING));
        assertTrue(states.contains(PipelineTask.State.ERROR));
    }

    /**
     * Tests selection of the max value of a column.
     */
    @Test
    public void testMax() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.column(PipelineTask_.id).max();
        Long maxId = (Long) DatabaseTransactionFactory
            .performTransaction(() -> crud.uniqueResult(query));
        assertEquals(4L, maxId.longValue());
    }

    /**
     * Tests selection of the min value of a column.
     */
    @Test
    public void testMin() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.column(PipelineTask_.id).min();
        Long minId = (Long) DatabaseTransactionFactory
            .performTransaction(() -> crud.uniqueResult(query));
        assertEquals(1L, minId.longValue());
    }

    /**
     * tests selection of the sum of a column.
     */
    @Test
    public void testSum() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.column(PipelineTask_.id).sum();
        Long idSum = (Long) DatabaseTransactionFactory
            .performTransaction(() -> crud.uniqueResult(query));
        assertEquals(10L, idSum.longValue());
    }

    /**
     * Tests selection of mulitple columns.
     */
    @Test
    public void testMultiSelect() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Object[]> query = crud.createZiggyQuery(PipelineTask.class,
            Object[].class);
        query.column(PipelineTask_.id).select();
        query.column(PipelineTask_.state).select();
        query.column(PipelineTask_.id).descendingOrder();
        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, results.size());
        assertEquals(4L, ((Long) results.get(0)[0]).longValue());
        assertEquals(PipelineTask.State.COMPLETED, results.get(0)[1]);
        assertEquals(3L, ((Long) results.get(1)[0]).longValue());
        assertEquals(PipelineTask.State.ERROR, results.get(1)[1]);
        assertEquals(2L, ((Long) results.get(2)[0]).longValue());
        assertEquals(PipelineTask.State.PROCESSING, results.get(2)[1]);
        assertEquals(1L, ((Long) results.get(3)[0]).longValue());
        assertEquals(PipelineTask.State.COMPLETED, results.get(3)[1]);
    }

    /**
     * Test of simultaneous min and max selection.
     */
    @Test
    public void testMinMax() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Object[]> query = crud.createZiggyQuery(PipelineTask.class,
            Object[].class);
        query.column(PipelineTask_.id).minMax();
        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(1, results.size());
        assertEquals(1L, ((Long) results.get(0)[0]).longValue());
        assertEquals(4L, ((Long) results.get(0)[1]).longValue());
    }

    /**
     * Tests a query in which both a where clause and a select clause are used.
     */
    @Test
    public void testCombineWhereSelect() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.column(PipelineTask_.state).in(PipelineTask.State.COMPLETED);
        query.column(PipelineTask_.id).select();
        @SuppressWarnings("unchecked")
        List<Long> results = (List<Long>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(2, results.size());
        assertTrue(results.contains(1L));
        assertTrue(results.contains(4L));
    }

    @Test
    public void testWhereScalarInScalar() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, State> query = crud
            .createZiggyQuery(PipelineTask.class, State.class)
            .column(PipelineTask_.state)
            .select()
            .in(PipelineTask.State.ERROR);
        @SuppressWarnings("unchecked")
        List<State> results = (List<State>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(1, results.size());
        assertTrue(results.contains(State.ERROR));
    }

    @Test(expected = IllegalStateException.class)
    public void testWhereCollectionInScalar() {
        populatePipelineTasks();
        crud.createZiggyQuery(PipelineTask.class, Long.class)
            .column(PipelineTask_.producerTaskIds)
            .select()
            .in(42L);
    }

    /**
     * Tests that the {#link {@link ZiggyQuery#chunkedIn(java.util.Collection)}} works as expected.
     */
    @Test
    public void testChunkedIn() {
        populatePipelineTasks();
        final ZiggyQuery<PipelineTask, Long> query = Mockito
            .spy(crud.createZiggyQuery(PipelineTask.class, Long.class));
        Mockito.doReturn(2).when(query).maxExpressions();
        query.column(PipelineTask_.id).select().chunkedIn(Set.of(1L, 2L, 3L, 4L));
        @SuppressWarnings("unchecked")
        List<Long> results = (List<Long>) DatabaseTransactionFactory
            .performTransaction(() -> crud.list(query));
        assertEquals(4, results.size());
        assertTrue(results.contains(1L));
        assertTrue(results.contains(2L));
        assertTrue(results.contains(3L));
        assertTrue(results.contains(4L));
        List<List<Object>> queryChunks = query.queryChunks();
        assertEquals(2, queryChunks.size());
        assertEquals(2, queryChunks.get(0).size());
        assertEquals(2, queryChunks.get(1).size());
        List<Object> allQueryChunks = new ArrayList<>(queryChunks.get(0));
        allQueryChunks.addAll(queryChunks.get(1));
        assertTrue(allQueryChunks.contains(1L));
        assertTrue(allQueryChunks.contains(2L));
        assertTrue(allQueryChunks.contains(3L));
        assertTrue(allQueryChunks.contains(4L));
    }

    /**
     * Exercises the {@link ZiggyQuery#count()} method.
     */
    @Test
    public void testCount() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Long> query = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        query.column(PipelineTask_.state).in(PipelineTask.State.COMPLETED);
        query.count();
        Long resultCount = (Long) DatabaseTransactionFactory
            .performTransaction(() -> crud.uniqueResult(query));
        assertEquals(2L, resultCount.longValue());
    }

    private List<String> logFileContents() throws IOException {
        return StringUtils
            .breakStringAtLineTerminations(Files.readString(logPath, FileUtil.ZIGGY_CHARSET));
    }

    private void populatePipelineTasks() {

        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineTask pipelineTask = new PipelineTask();
            pipelineTask.setState(PipelineTask.State.COMPLETED);
            pipelineTask.addProducerTaskId(40);
            crud.persist(pipelineTask);
            pipelineTask = new PipelineTask();
            pipelineTask.setState(PipelineTask.State.PROCESSING);
            pipelineTask.addProducerTaskId(41);
            crud.persist(pipelineTask);
            pipelineTask = new PipelineTask();
            pipelineTask.setState(PipelineTask.State.ERROR);
            pipelineTask.addProducerTaskId(42);
            crud.persist(pipelineTask);
            pipelineTask = new PipelineTask();
            pipelineTask.setState(PipelineTask.State.COMPLETED);
            pipelineTask.addProducerTaskId(43);
            crud.persist(pipelineTask);
            return null;
        });
    }
}
