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

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskData_;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.UniqueNameVersionPipelineComponent_;
import gov.nasa.ziggy.pipeline.definition.database.PipelineDefinitionCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceNodeCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineOperationsTestUtils;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
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
    private final String TABLE_NAME = "ziggy_PipelineTaskData";
    private final String DUMMY_TASK_NAME = "p1_0";

    private PipelineTaskDataCrud crud;
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
        crud = new PipelineTaskDataCrud();

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

        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
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

        // Get the fields for PipelineTaskData, removing any fields that are lazy initialized,
        // transient, or embedded as they don't appear in the query.
        Field[] fields = PipelineTaskData.class.getDeclaredFields();
        List<String> fieldNames = new ArrayList<>();
        Set<String> otherFieldNames = Set.of("executionClock", "pipelineTaskMetrics",
            "taskExecutionLogs", "remoteJobs");

        for (Field field : fields) {
            if (!otherFieldNames.contains(field.getName())) {
                fieldNames.add(field.getName());
            }
        }

        // All the non-lazy fields should appear between the select statement and the from
        // statement.
        for (String fieldName : fieldNames) {
            String fullFieldName = DUMMY_TASK_NAME + "." + fieldName;
            int fullFieldNameIndex = queryString.indexOf(fullFieldName);
            assertTrue(fieldName,
                fullFieldNameIndex > queryStart && fullFieldNameIndex < tableNameStart);
        }

        // None of the lazy, transient, or embedded fields should appear at all.
        for (String fieldName : otherFieldNames) {
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
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(4, tasks.size());
    }

    /**
     * Tests that a single-valued column criterion behaves as expected.
     */
    @Test
    public void testColumnCriterion() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.column(PipelineTaskData_.processingStep).in(ProcessingStep.COMPLETE);
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
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
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.where(query.getBuilder()
            .between(query.getRoot().get(PipelineTaskData_.pipelineTaskId), 2L, 3L));
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
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
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.where(query.in(query.get(PipelineTaskData_.pipelineTaskId), Set.of(1L, 2L)));
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(2, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
        assertTrue(taskIds.contains(1L));
        assertTrue(taskIds.contains(2L));
    }

    /**
     * Tests the {@link ZiggyQuery#in(jakarta.persistence.criteria.Expression, Object)} method.
     */
    @Test
    public void testScalarInClause() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.where(query.in(query.get(PipelineTaskData_.pipelineTaskId), 1L));
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(1, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
        assertTrue(taskIds.contains(1L));
    }

    /**
     * Tests that a multi-valued column criterion behaves as expected (i.e., any task that matches
     * any of the criteria is returned).
     */
    @Test
    public void testColumnMultiValueCriterion() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.column(PipelineTaskData_.processingStep)
            .in(Set.of(ProcessingStep.COMPLETE, ProcessingStep.EXECUTING));
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(4, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
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
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.column(PipelineTaskData_.pipelineTaskId).in(Set.of(2L, 4L)).between(1L, 3L);
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(1, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
        assertTrue(taskIds.contains(2L));
    }

    /**
     * Tests that criteria on multiple columns are combined by an AND operation.
     */
    @Test
    public void testCriteriaOnMultipleColumns() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.column(PipelineTaskData_.pipelineTaskId)
            .in(Set.of(2L, 3L, 4L))
            .column(PipelineTaskData_.processingStep)
            .in(ProcessingStep.COMPLETE);
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(1, tasks.size());
        List<Long> taskIds = tasks.stream()
            .map(t -> t.getPipelineTask().getId())
            .collect(Collectors.toList());
        assertTrue(taskIds.contains(4L));
    }

    /**
     * Tests that a descending sort works as expected.
     */
    @Test
    public void testSort() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, PipelineTaskData> query = crud
            .createZiggyQuery(PipelineTaskData.class);
        query.column(PipelineTaskData_.pipelineTaskId).descendingOrder();
        List<PipelineTaskData> tasks = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
        assertEquals(4, tasks.size());
        assertEquals(4L, tasks.get(0).getPipelineTask().getId().longValue());
        assertEquals(3L, tasks.get(1).getPipelineTask().getId().longValue());
        assertEquals(2L, tasks.get(2).getPipelineTask().getId().longValue());
        assertEquals(1L, tasks.get(3).getPipelineTask().getId().longValue());
    }

    /**
     * Tests that a single-column select operation behaves as expected.
     */
    @Test
    public void testSelectColumn() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.column(PipelineTaskData_.pipelineTaskId).select();
        List<Long> ids = testOperations.performListQueryOnPipelineTaskDataTable(query);
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
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.select(PipelineTaskData_.pipelineTaskId);
        List<Long> ids = testOperations.performListQueryOnPipelineTaskDataTable(query);
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
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.select(query.getRoot().get("id"));
        List<Long> ids = testOperations.performListQueryOnPipelineTaskDataTable(query);
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
        ZiggyQuery<PipelineTaskData, ProcessingStep> query = crud
            .createZiggyQuery(PipelineTaskData.class, ProcessingStep.class);
        query.column(PipelineTaskData_.processingStep).select();
        List<ProcessingStep> steps = testOperations.performListQueryOnPipelineTaskDataTable(query);
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
        ZiggyQuery<PipelineTaskData, ProcessingStep> query = crud
            .createZiggyQuery(PipelineTaskData.class, ProcessingStep.class);
        query.column(PipelineTaskData_.processingStep).select();
        query.distinct(true);
        List<ProcessingStep> steps = testOperations.performListQueryOnPipelineTaskDataTable(query);
        assertEquals(2, steps.size());
        assertTrue(steps.contains(ProcessingStep.COMPLETE));
        assertTrue(steps.contains(ProcessingStep.EXECUTING));
        assertErrorCount(1);
    }

    private void assertErrorCount(int expectedErrors) {
        ZiggyQuery<PipelineTaskData, Long> errorCountQuery = crud
            .createZiggyQuery(PipelineTaskData.class, Long.class);
        errorCountQuery.column(PipelineTaskData_.error).in(true).count();
        long errorCount = testOperations
            .performUniqueResultQueryOnPipelineTaskDataTable(errorCountQuery);
        assertEquals(expectedErrors, errorCount);
    }

    /**
     * Tests selection of the max value of a column.
     */
    @Test
    public void testMax() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.column(PipelineTaskData_.pipelineTaskId).max();
        Long maxId = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
        assertEquals(4L, maxId.longValue());
    }

    /**
     * Tests selection of the min value of a column.
     */
    @Test
    public void testMin() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.column(PipelineTaskData_.pipelineTaskId).min();
        Long minId = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
        assertEquals(1L, minId.longValue());
    }

    /**
     * tests selection of the sum of a column.
     */
    @Test
    public void testSum() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.column(PipelineTaskData_.pipelineTaskId).sum();
        Long idSum = testOperations.performUniqueResultQueryOnPipelineTaskTable(query);
        assertEquals(10L, idSum.longValue());
    }

    /**
     * Tests selection of multiple columns.
     */
    @Test
    public void testMultiSelect() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Object[]> query = crud.createZiggyQuery(PipelineTaskData.class,
            Object[].class);
        query.column(PipelineTaskData_.pipelineTaskId).select();
        query.column(PipelineTaskData_.processingStep).select();
        query.column(PipelineTaskData_.pipelineTaskId).descendingOrder();
        List<Object[]> results = testOperations.performListQueryOnPipelineTaskDataTable(query);
        assertEquals(4, results.size());
        assertEquals(4L, ((Long) results.get(0)[0]).longValue());
        assertEquals(ProcessingStep.COMPLETE, results.get(0)[1]);
        assertEquals(3L, ((Long) results.get(1)[0]).longValue());
        assertEquals(ProcessingStep.EXECUTING, results.get(1)[1]);
        assertEquals(2L, ((Long) results.get(2)[0]).longValue());
        assertEquals(ProcessingStep.EXECUTING, results.get(2)[1]);
        assertEquals(1L, ((Long) results.get(3)[0]).longValue());
        assertEquals(ProcessingStep.COMPLETE, results.get(3)[1]);

        query = crud.createZiggyQuery(PipelineTaskData.class, Object[].class);
        query.column(PipelineTaskData_.pipelineTaskId).select();
        query.column(PipelineTaskData_.error).in(true);
        results = testOperations.performListQueryOnPipelineTaskDataTable(query);
        assertEquals(1, results.size());
        assertEquals(3L, ((Long) results.get(0)[0]).longValue());
    }

    /**
     * Test of simultaneous min and max selection.
     */
    @Test
    public void testMinMax() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Object[]> query = crud.createZiggyQuery(PipelineTaskData.class,
            Object[].class);
        query.column(PipelineTaskData_.pipelineTaskId).minMax();
        List<Object[]> results = testOperations.performListQueryOnPipelineTaskDataTable(query);
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
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.column(PipelineTaskData_.processingStep).in(ProcessingStep.COMPLETE);
        query.column(PipelineTaskData_.pipelineTaskId).select();
        List<Long> results = testOperations.performListQueryOnPipelineTaskDataTable(query);
        assertEquals(2, results.size());
        assertTrue(results.contains(1L));
        assertTrue(results.contains(4L));
    }

    @Test
    public void testWhereScalarInScalar() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, ProcessingStep> query = crud
            .createZiggyQuery(PipelineTaskData.class, ProcessingStep.class)
            .column(PipelineTaskData_.processingStep)
            .select()
            .in(ProcessingStep.COMPLETE);
        List<ProcessingStep> results = testOperations
            .performListQueryOnPipelineTaskDataTable(query);
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
     * Exercises the {@link ZiggyQuery#count()} method.
     */
    @Test
    public void testCount() {
        populatePipelineTasks();
        ZiggyQuery<PipelineTaskData, Long> query = crud.createZiggyQuery(PipelineTaskData.class,
            Long.class);
        query.column(PipelineTaskData_.processingStep).in(ProcessingStep.COMPLETE);
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
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.COMPLETE, false,
            pipelineInstanceNode);
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.EXECUTING, false,
            pipelineInstanceNode);
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.EXECUTING, true,
            pipelineInstanceNode);
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.COMPLETE, false,
            pipelineInstanceNode);
        testOperations.merge(pipelineInstanceNode);
    }

    private void populatePipelineTasksWithTwoInstances() {

        PipelineInstanceNode pipelineInstanceNode1 = testOperations
            .merge(new PipelineInstanceNode());
        PipelineInstanceNode pipelineInstanceNode2 = testOperations
            .merge(new PipelineInstanceNode());

        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.COMPLETE, false,
            pipelineInstanceNode1);
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.EXECUTING, false,
            pipelineInstanceNode1);
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.EXECUTING, true,
            pipelineInstanceNode2);
        testOperations.persistPipelineTask(new PipelineTask(), ProcessingStep.COMPLETE, false,
            pipelineInstanceNode2);

        testOperations.merge(pipelineInstanceNode1);
        testOperations.merge(pipelineInstanceNode2);
    }

    private class TestOperations extends DatabaseOperations {

        public <T> List<T> performListQueryOnPipelineTaskTable(ZiggyQuery<PipelineTask, T> query) {
            return performTransaction(() -> new PipelineTaskCrud().list(query));
        }

        public <T> List<T> performListQueryOnPipelineTaskDataTable(
            ZiggyQuery<PipelineTaskData, T> query) {
            return performTransaction(() -> new PipelineTaskDataCrud().list(query));
        }

        public <T> T performUniqueResultQueryOnPipelineTaskTable(
            ZiggyQuery<PipelineTaskData, T> query) {
            return performTransaction(() -> new PipelineTaskCrud().uniqueResult(query));
        }

        public <T> T performUniqueResultQueryOnPipelineTaskDataTable(
            ZiggyQuery<PipelineTaskData, T> query) {
            return performTransaction(() -> new PipelineTaskDataCrud().uniqueResult(query));
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

        protected PipelineTask persistPipelineTask(PipelineTask pipelineTask,
            ProcessingStep processingStep, boolean error,
            PipelineInstanceNode pipelineInstanceNode) {

            PipelineTask mergedTask = performTransaction(() -> {
                PipelineTask task = new PipelineTaskCrud().merge(pipelineTask);
                new PipelineTaskDataOperations().createPipelineTaskData(task, processingStep);
                new PipelineTaskDataOperations().setError(task, error);
                return task;
            });
            pipelineInstanceNode.addPipelineTask(mergedTask);
            pipelineTasks.add(mergedTask);
            return mergedTask;
        }

        public PipelineInstanceNode merge(PipelineInstanceNode pipelineInstanceNode) {
            return performTransaction(
                () -> new PipelineInstanceNodeCrud().merge(pipelineInstanceNode));
        }
    }
}
