package gov.nasa.ziggy.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.UniqueNameVersionPipelineComponent_;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.util.ZiggyStringUtils;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

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
    private TestOperations testOperations = new TestOperations();
    private List<PipelineTask> pipelineTasks;

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

    public ZiggyPropertyRule log4jLogFileProperty = new ZiggyPropertyRule("ziggy.logFile",
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
        pipelineTasks = new ArrayList<>();
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        Set<String> transientFieldNames = Set.of("maxFailedSubtaskCount", "maxAutoResubmits",
            "ERROR_PREFIX", "LOG_FILENAME_FORMAT");

        for (Field field : fields) {
            if (!lazyFieldNames.contains(field.getName())
                && !transientFieldNames.contains(field.getName())) {
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(4, tasks.size());
    }

    /**
     * Tests that a single-valued column criterion behaves as expected.
     */
    @Test
    public void testColumnCriterion() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, PipelineTask> query = crud.createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.processingStep).in(ProcessingStep.COMPLETE);
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        query.column(PipelineTask_.processingStep)
            .in(Set.of(ProcessingStep.COMPLETE, ProcessingStep.EXECUTING));
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(4, tasks.size());
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
            .column(PipelineTask_.processingStep)
            .in(ProcessingStep.COMPLETE);
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<PipelineTask> tasks = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<Long> ids = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<Long> ids = testOperations.performListQueryOnPipelineTaskTable(query);
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
        List<Long> ids = testOperations.performListQueryOnPipelineTaskTable(query);
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
        ZiggyQuery<PipelineTask, ProcessingStep> query = crud.createZiggyQuery(PipelineTask.class,
            ProcessingStep.class);
        query.column(PipelineTask_.processingStep).select();
        List<ProcessingStep> steps = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(4, steps.size());
        assertTrue(steps.contains(ProcessingStep.COMPLETE));
        assertTrue(steps.contains(ProcessingStep.EXECUTING));
        assertErrorCount(1);
    }

    /**
     * Tests that distinct selection works as expected.
     */
    @Test
    public void testDistinctSelection() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, ProcessingStep> query = crud.createZiggyQuery(PipelineTask.class,
            ProcessingStep.class);
        query.column(PipelineTask_.processingStep).select();
        query.distinct(true);
        List<ProcessingStep> steps = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(2, steps.size());
        assertTrue(steps.contains(ProcessingStep.COMPLETE));
        assertTrue(steps.contains(ProcessingStep.EXECUTING));
        assertErrorCount(1);
    }

    private void assertErrorCount(int expectedErrors) {
        ZiggyQuery<PipelineTask, Long> errorCountQuery = crud.createZiggyQuery(PipelineTask.class,
            Long.class);
        errorCountQuery.column(PipelineTask_.error).in(true).count();
        long errorCount = testOperations
            .performUniqueResultQueryOnPipelineTaskTable(errorCountQuery);
        assertEquals(expectedErrors, errorCount);
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
        Long maxId = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
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
        Long minId = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
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
        Long idSum = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
        assertEquals(10L, idSum.longValue());
    }

    /**
     * Tests selection of multiple columns.
     */
    @Test
    public void testMultiSelect() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, Object[]> query = crud.createZiggyQuery(PipelineTask.class,
            Object[].class);
        query.column(PipelineTask_.id).select();
        query.column(PipelineTask_.processingStep).select();
        query.column(PipelineTask_.id).descendingOrder();
        List<Object[]> results = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(4, results.size());
        assertEquals(4L, ((Long) results.get(0)[0]).longValue());
        assertEquals(ProcessingStep.COMPLETE, results.get(0)[1]);
        assertEquals(3L, ((Long) results.get(1)[0]).longValue());
        assertEquals(ProcessingStep.EXECUTING, results.get(1)[1]);
        assertEquals(2L, ((Long) results.get(2)[0]).longValue());
        assertEquals(ProcessingStep.EXECUTING, results.get(2)[1]);
        assertEquals(1L, ((Long) results.get(3)[0]).longValue());
        assertEquals(ProcessingStep.COMPLETE, results.get(3)[1]);

        query = crud.createZiggyQuery(PipelineTask.class, Object[].class);
        query.column(PipelineTask_.id).select();
        query.column(PipelineTask_.error).in(true);
        results = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(1, results.size());
        assertEquals(3L, ((Long) results.get(0)[0]).longValue());
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
        List<Object[]> results = testOperations.performListQueryOnPipelineTaskTable(query);
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
        query.column(PipelineTask_.processingStep).in(ProcessingStep.COMPLETE);
        query.column(PipelineTask_.id).select();
        List<Long> results = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(2, results.size());
        assertTrue(results.contains(1L));
        assertTrue(results.contains(4L));
    }

    @Test
    public void testWhereScalarInScalar() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTask, ProcessingStep> query = crud
            .createZiggyQuery(PipelineTask.class, ProcessingStep.class)
            .column(PipelineTask_.processingStep)
            .select()
            .in(ProcessingStep.COMPLETE);
        List<ProcessingStep> results = testOperations.performListQueryOnPipelineTaskTable(query);
        assertEquals(2, results.size());
        assertTrue(results.contains(ProcessingStep.COMPLETE));
    }

    @Test(expected = IllegalStateException.class)
    public void testWhereCollectionInScalar() {
        populatePipelineTasks();
        crud.createZiggyQuery(PipelineInstanceNode.class, Long.class)
            .column(PipelineInstanceNode_.id)
            .select()
            .column(PipelineInstanceNode_.pipelineTasks)
            .in(pipelineTasks.get(0));
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
        List<Long> results = testOperations.performListQueryOnPipelineTaskTable(query);
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
        query.column(PipelineTask_.processingStep).in(ProcessingStep.COMPLETE);
        query.count();
        Long resultCount = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
        assertEquals(2L, resultCount.longValue());
    }

    /**
     * Exercises the {@link ZiggyQuery#contains(Object)} method. Implicitly tests the
     * {@link ZiggyQuery#join()} method, which is used internally by contains.
     */
    @Test
    public void testContains() {
        populatePipelineTasks();
        List<PipelineTask> pipelineTasks = testOperations
            .performListQueryOnPipelineTaskTable(crud.createZiggyQuery(PipelineTask.class));
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = crud
            .createZiggyQuery(PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTasks.get(0));
        PipelineInstanceNode pipelineInstanceNode = testOperations
            .performUniqueResultQueryOnPipelineInstanceNodeTable(query);
        assertNotNull(pipelineInstanceNode);
        assertEquals(1L, pipelineInstanceNode.getId().longValue());
    }

    /**
     * Exercises the {@link ZiggyQuery#containsAny(java.util.Collection)} method. Implicitly tests
     * the {@link ZiggyQuery#join()} method, which is used internally by containsAny.
     */
    @Test
    public void testContainsAny() {
        populatePipelineTasksWithTwoInstances();
        List<PipelineTask> pipelineTasks = testOperations
            .performListQueryOnPipelineTaskTable(crud.createZiggyQuery(PipelineTask.class));
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = crud
            .createZiggyQuery(PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineTasks).containsAny(pipelineTasks);
        List<PipelineInstanceNode> pipelineInstanceNodes = testOperations
            .performListQueryOnPipelineInstanceNodeTable(query);
        assertFalse(pipelineInstanceNodes.isEmpty());
        assertEquals(2, pipelineInstanceNodes.size());
        List<Long> pipelineInstanceNodeIds = pipelineInstanceNodes.stream()
            .map(PipelineInstanceNode::getId)
            .collect(Collectors.toList());
        assertTrue(pipelineInstanceNodeIds.contains(1L));
        assertTrue(pipelineInstanceNodeIds.contains(2L));
    }

    @Test
    public void testContainsAnyNullArgument() {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = crud
            .createZiggyQuery(PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineTasks).containsAny(null);
        List<PipelineInstanceNode> pipelineInstanceNodes = testOperations
            .performListQueryOnPipelineInstanceNodeTable(query);
        assertTrue(pipelineInstanceNodes.isEmpty());
    }

    @Test
    public void testSubquery() {
        PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
        pipelineOperationsTestUtils.setUpSingleModulePipeline();
        ZiggyQuery<PipelineDefinition, PipelineDefinition> query = crud
            .createZiggyQuery(PipelineDefinition.class);

        // Find the maximum index of the selected name via a subquery. We need to use a
        // subquery because the max() method automatically implements a select of the column
        // that's being searched.
        ZiggyQuery<PipelineDefinition, Integer> subquery = query
            .ziggySubquery(PipelineDefinition.class, Integer.class);
        subquery.column(UniqueNameVersionPipelineComponent_.NAME)
            .in(pipelineOperationsTestUtils.pipelineDefinition().getName());
        subquery.column(UniqueNameVersionPipelineComponent_.VERSION).max();

        // Use the subquery result to find the correct pipeline definition version.
        query.column(UniqueNameVersionPipelineComponent_.NAME)
            .in(pipelineOperationsTestUtils.pipelineDefinition().getName());
        query.column(UniqueNameVersionPipelineComponent_.VERSION).in(subquery);
        PipelineDefinition retrievedDefinition = testOperations
            .performUniqueResultQueryOnPipelineDefinitionTable(query);
        assertNotNull(retrievedDefinition);
        assertEquals(1L, retrievedDefinition.getId().longValue());
        assertEquals(0, retrievedDefinition.getVersion());
    }

    private List<String> logFileContents() throws IOException {
        return ZiggyStringUtils
            .splitStringAtLineTerminations(Files.readString(logPath, ZiggyFileUtils.ZIGGY_CHARSET));
    }

    private void populatePipelineTasks() {

        PipelineInstanceNode pipelineInstanceNode = testOperations
            .merge(new PipelineInstanceNode());
        PipelineTask pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.COMPLETE);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode);
        pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.EXECUTING);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode);
        pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.EXECUTING);
        pipelineTask.setError(true);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode);
        pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.COMPLETE);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode);
        testOperations.merge(pipelineInstanceNode);
    }

    private void populatePipelineTasksWithTwoInstances() {

        PipelineInstanceNode pipelineInstanceNode1 = testOperations
            .merge(new PipelineInstanceNode());
        PipelineInstanceNode pipelineInstanceNode2 = testOperations
            .merge(new PipelineInstanceNode());

        PipelineTask pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.COMPLETE);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode1);
        pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.EXECUTING);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode1);
        pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.EXECUTING);
        pipelineTask.setError(true);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode2);
        pipelineTask = new PipelineTask();
        pipelineTask.setProcessingStep(ProcessingStep.COMPLETE);
        testOperations.persistPipelineTask(pipelineTask, pipelineInstanceNode2);
        testOperations.merge(pipelineInstanceNode1);
        testOperations.merge(pipelineInstanceNode2);
    }

    private class TestOperations extends DatabaseOperations {

        public <T> List<T> performListQueryOnPipelineTaskTable(ZiggyQuery<PipelineTask, T> query) {
            return performTransaction(() -> new PipelineTaskCrud().list(query));
        }

        public <T> T performUniqueResultQueryOnPipelineTaskTable(
            ZiggyQuery<PipelineTask, T> query) {
            return performTransaction(() -> new PipelineTaskCrud().uniqueResult(query));
        }

        public <T> T performUniqueResultQueryOnPipelineInstanceNodeTable(
            ZiggyQuery<PipelineInstanceNode, T> query) {
            return performTransaction(() -> new PipelineInstanceNodeCrud().uniqueResult(query));
        }

        public <T> List<T> performListQueryOnPipelineInstanceNodeTable(
            ZiggyQuery<PipelineInstanceNode, T> query) {
            return performTransaction(() -> new PipelineInstanceNodeCrud().list(query));
        }

        public <T> T performUniqueResultQueryOnPipelineDefinitionTable(
            ZiggyQuery<PipelineDefinition, T> query) {
            return performTransaction(() -> new PipelineDefinitionCrud().uniqueResult(query));
        }

        public void persistPipelineTask(PipelineTask pipelineTask,
            PipelineInstanceNode pipelineInstanceNode) {
            PipelineTask mergedTask = performTransaction(
                () -> new PipelineTaskCrud().merge(pipelineTask));
            pipelineInstanceNode.addPipelineTask(mergedTask);
            pipelineTasks.add(mergedTask);
        }

        public PipelineInstanceNode merge(PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().merge(pipelineInstanceNode));
        }
    }
}
