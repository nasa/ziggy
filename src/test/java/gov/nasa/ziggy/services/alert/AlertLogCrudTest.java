package gov.nasa.ziggy.services.alert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.Alert.Severity;
import gov.nasa.ziggy.services.database.DatabaseOperations;

// TODO Rename to AlertLogOperationsTest and adjust
//
// Only Operations classes should extend DatabaseOperations classes. CRUD classes should not be
// tested directly, but indirectly through their associated Operations class.
//
// If there is test code that isn't appropriate for a production operations class, move it to an
// inner class called TestOperations that extends DatabaseOperations.

public class AlertLogCrudTest {
    private TestOperations testOperations = new TestOperations();

    private final SimpleDateFormat parser = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");

    private Date date1;
    private Date date2;
    private Date date4;
    private Date date5;
    private Date date6;
    private Date date7;

    private PipelineTask task1;
    private PipelineTask task3;
    private PipelineTask task4;
    private PipelineTask task5;
    private PipelineInstance instance1;
    private PipelineInstance instance2;

    private AlertLogOperations alertLogOperations = new AlertLogOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() throws Exception {

        date1 = parser.parse("Jun-1-12 12:00:00");
        date2 = parser.parse("Jun-2-12 12:00:00");
        date4 = parser.parse("Aug-12-12 02:00:00");
        date5 = parser.parse("Sep-20-12 05:00:00");
        date6 = parser.parse("Sep-21-12 05:00:00");
        date7 = parser.parse("Oct-31-12 19:00:00");
    }

    @Test
    public void testRetrieveComponents() {

        List<String> components = testOperations.alertComponents();
        assertEquals(0, components.size());

        populateObjects();
        components = testOperations.alertComponents();

        // Check number of components as well as sort.
        assertEquals(6, components.size());
        assertEquals("s1", components.get(0));
        assertEquals("s2", components.get(1));
        assertEquals("s4", components.get(2));
        assertEquals("s5", components.get(3));
        assertEquals("s6", components.get(4));
        assertEquals("s8", components.get(5));
    }

    @Test
    public void testRetrieveSeverities() {
        List<Severity> severities = testOperations.alertSeverities();
        assertEquals(0, severities.size());
        populateObjects();

        severities = testOperations.alertSeverities();

        // Check number of components as well as sort.
        assertEquals(3, severities.size());
        assertEquals(Severity.ERROR, severities.get(0));
        assertEquals(Severity.INFRASTRUCTURE, severities.get(1));
        assertEquals(Severity.WARNING, severities.get(2));
    }

    @Test
    public void testCreateRetrieve() throws Exception {
        populateObjects();

        List<AlertLog> alerts = testOperations.alertsInDateRange(date2, date6);
        assertEquals("alerts.size()", 4, alerts.size());
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullStart() {
        testOperations.alerts(null, null, null, null);
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullEnd() {
        testOperations.alerts(new Date(), null, null, null);
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullComponent() {
        testOperations.alerts(new Date(), new Date(), null, null);
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullSeverity() {
        testOperations.alerts(new Date(), new Date(), List.of(), null);
    }

    @Test
    public void testRetrieveAlertsByTask() {
        populateObjects();
        List<AlertLog> alerts = testOperations.alertsForPipelineTasks(List.of(task5, task1));
        assertEquals(4, alerts.size());
    }

    @Test
    public void testRetrieve() {
        populateObjects();

        // Test that empty components means that all components are considered.
        List<String> components = List.of();
        List<Severity> severities = List.of();
        List<AlertLog> alerts = testOperations.alerts(date2, date6, components, severities);
        assertEquals(4, alerts.size());

        // This component isn't in the date range.
        components = List.of("s4");
        alerts = testOperations.alerts(date2, date6, components, severities);
        assertEquals(0, alerts.size());

        // This component has one entry in and one outside of the range.
        components = List.of("s1");
        alerts = testOperations.alerts(date2, date6, components, severities);
        assertEquals(1, alerts.size());
        assertEquals("D", alerts.get(0).getAlertData().getProcessName());

        // Specify all components; check sort.
        components = List.of("s1", "s2", "s4");
        severities = List.of(Severity.ERROR, Severity.ERROR, Severity.INFRASTRUCTURE,
            Severity.WARNING, Severity.ERROR);
        alerts = testOperations.alerts(date1, date7, components, severities);
        assertEquals(4, alerts.size());
        assertEquals("D", alerts.get(0).getAlertData().getProcessName());
        assertEquals("E", alerts.get(1).getAlertData().getProcessName());
        assertEquals("C", alerts.get(2).getAlertData().getProcessName());
        assertEquals("A", alerts.get(3).getAlertData().getProcessName());
        assertEquals(Severity.ERROR, alerts.get(0).getAlertData().getSeverity());

        components = List.of("s5", "s6", "s8");
        alerts = testOperations.alerts(date1, date7, components, severities);
        assertEquals(4, alerts.size());
        assertEquals(Severity.ERROR, alerts.get(0).getAlertData().getSeverity());
        assertEquals(Severity.ERROR, alerts.get(1).getAlertData().getSeverity());
        assertEquals(Severity.INFRASTRUCTURE, alerts.get(2).getAlertData().getSeverity());
        assertEquals(Severity.WARNING, alerts.get(3).getAlertData().getSeverity());

        // Check just the SEVERE severity.
        severities = List.of(Severity.ERROR);
        alerts = testOperations.alerts(date1, date7, components, severities);
        assertEquals(2, alerts.size());
        assertEquals(Severity.ERROR, alerts.get(0).getAlertData().getSeverity());
    }

    @Test
    public void testRetrieveForPipelineInstance() {
        populateObjects();

        List<AlertLog> logs = alertLogOperations().alertLogs(instance1);
        assertEquals(2, logs.size());
        Alert alert = null;
        for (AlertLog log : logs) {
            if (log.getAlertData().getProcessName().equals("AW")) {
                alert = log.getAlertData();
            }
        }
        assertNotNull(alert);
        assertEquals("s8", alert.getSourceComponent());
        assertEquals("AW", alert.getProcessName());
        assertEquals("a", alert.getProcessHost());
        assertEquals(1L, alert.getProcessId());
        assertEquals("message1", alert.getMessage());
        assertEquals(1L, alert.getSourceTask().getId().longValue());
        assertEquals(1L, alert.getSourceTask().getPipelineInstanceId());
        assertEquals(date1, alert.getTimestamp());
    }

    private void populateObjects() {
        instance1 = testOperations.merge(new PipelineInstance());
        task1 = new PipelineTask(instance1, null, null);

        instance2 = testOperations.merge(new PipelineInstance());
        task3 = new PipelineTask(instance2, null, null);
        task4 = new PipelineTask(instance2, null, null);
        task5 = new PipelineTask(instance2, null, null);

        testOperations.persistPipelineTask(task1);
        testOperations.persistPipelineTask(task3);
        testOperations.persistPipelineTask(task4);
        testOperations.persistPipelineTask(task5);

        alertLogOperations().persist(
            new AlertLog(new Alert(date7, "s1", task5, "E", "e", 5, Severity.ERROR, "message5")));
        alertLogOperations().persist(
            new AlertLog(new Alert(date5, "s1", task4, "D", "d", 4, Severity.ERROR, "message4")));
        alertLogOperations().persist(new AlertLog(
            new Alert(date4, "s2", task3, "C", "c", 3, Severity.INFRASTRUCTURE, "message3")));
        alertLogOperations().persist(
            new AlertLog(new Alert(date1, "s4", task1, "A", "a", 1, Severity.WARNING, "message1")));

        alertLogOperations().persist(
            new AlertLog(new Alert(date5, "s5", task4, "DS", "d", 4, Severity.ERROR, "message4")));
        alertLogOperations().persist(
            new AlertLog(new Alert(date7, "s5", task5, "EF", "e", 5, Severity.ERROR, "message5")));
        alertLogOperations().persist(new AlertLog(
            new Alert(date4, "s6", task3, "CD", "c", 3, Severity.INFRASTRUCTURE, "message3")));
        alertLogOperations().persist(new AlertLog(
            new Alert(date1, "s8", task1, "AW", "a", 1, Severity.WARNING, "message1")));
    }

    AlertLogOperations alertLogOperations() {
        return alertLogOperations;
    }

    private static class TestOperations extends DatabaseOperations {

        public List<String> alertComponents() {
            return performTransaction(() -> new AlertLogCrud().retrieveComponents());
        }

        public List<Severity> alertSeverities() {
            return performTransaction(() -> new AlertLogCrud().retrieveSeverities());
        }

        public List<AlertLog> alertsInDateRange(Date startDate, Date endDate) {
            return performTransaction(() -> new AlertLogCrud().retrieve(startDate, endDate));
        }

        public List<AlertLog> alerts(Date startDate, Date endDate, List<String> components,
            List<Severity> severities) {
            return performTransaction(
                () -> new AlertLogCrud().retrieve(startDate, endDate, components, severities));
        }

        public List<AlertLog> alertsForPipelineTasks(List<PipelineTask> tasks) {
            return performTransaction(() -> new AlertLogCrud().retrieveByPipelineTasks(tasks));
        }

        public PipelineInstance merge(PipelineInstance pipelineInstance) {
            return performTransaction(() -> new PipelineInstanceCrud().merge(pipelineInstance));
        }

        public void persistPipelineTask(PipelineTask pipelineTask) {
            performTransaction(() -> new PipelineTaskCrud().persist(pipelineTask));
        }
    }
}
