package gov.nasa.ziggy.services.alert;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineInstanceCrud;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

public class AlertLogCrudTest {
    private AlertLogCrud alertCrud;

    private final SimpleDateFormat parser = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");

    private Date date1;
    private Date date2;
    private Date date3;
    private Date date4;
    private Date date5;
    private Date date6;
    private Date date7;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() throws Exception {
        alertCrud = new AlertLogCrud();

        date1 = parser.parse("Jun-1-12 12:00:00");
        date2 = parser.parse("Jun-2-12 12:00:00");
        date3 = parser.parse("Jul-10-12 15:00:00");
        date4 = parser.parse("Aug-12-12 02:00:00");
        date5 = parser.parse("Sep-20-12 05:00:00");
        date6 = parser.parse("Sep-21-12 05:00:00");
        date7 = parser.parse("Oct-31-12 19:00:00");
    }

    @Test
    public void testRetrieveComponents() {
        DatabaseTransactionFactory.performTransaction(() -> {
            List<String> components = alertCrud.retrieveComponents();
            assertEquals(0, components.size());
            return null;
        });

        populateObjects();
        DatabaseTransactionFactory.performTransaction(() -> {
            List<String> components = alertCrud.retrieveComponents();

            // Check number of components as well as sort.
            assertEquals(8, components.size());
            assertEquals("s1", components.get(0));
            assertEquals("s2", components.get(1));
            assertEquals("s3", components.get(2));
            assertEquals("s4", components.get(3));
            return null;
        });
    }

    @Test
    public void testRetrieveSeverities() {
        DatabaseTransactionFactory.performTransaction(() -> {
            List<String> severities = alertCrud.retrieveSeverities();
            assertEquals(0, severities.size());
            return null;
        });
        populateObjects();

        DatabaseTransactionFactory.performTransaction(() -> {
            List<String> severities = alertCrud.retrieveSeverities();

            // Check number of components as well as sort.
            assertEquals(4, severities.size());
            assertEquals(Level.DEBUG.toString(), severities.get(0));
            assertEquals(Level.ERROR.toString(), severities.get(1));
            assertEquals(Level.INFO.toString(), severities.get(2));
            assertEquals(Level.WARN.toString(), severities.get(3));
            return null;
        });
    }

    @Test
    public void testCreateRetrieve() throws Exception {
        populateObjects();

        DatabaseTransactionFactory.performTransaction(() -> {
            List<AlertLog> alerts = alertCrud.retrieve(date2, date6);
            assertEquals("alerts.size()", 6, alerts.size());
            return null;
        });
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullStart() {
        DatabaseTransactionFactory.performTransaction(() -> {
            alertCrud.retrieve(null, null, null, null);
            return null;
        });
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullEnd() {
        DatabaseTransactionFactory.performTransaction(() -> {
            alertCrud.retrieve(new Date(), null, null, null);
            return null;
        });
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullComponent() {
        DatabaseTransactionFactory.performTransaction(() -> {
            alertCrud.retrieve(new Date(), new Date(), null, null);
            return null;
        });
    }

    @Test(expected = PipelineException.class)
    public void testRetrieveNullSeverity() {
        DatabaseTransactionFactory.performTransaction(() -> {
            alertCrud.retrieve(new Date(), new Date(), new String[0], null);
            return null;
        });
    }

    @Test
    public void testRetrieveAlertsByTaskId() {
        populateObjects();

        List<Long> taskIds = new ArrayList<>();
        taskIds.add(5L);
        taskIds.add(1L);

        DatabaseTransactionFactory.performTransaction(() -> {
            List<AlertLog> alerts = alertCrud.retrieveByPipelineTaskIds(taskIds);
            assertEquals(4, alerts.size());
            return null;
        });
    }

    @Test
    public void testRetrieve() {
        populateObjects();

        DatabaseTransactionFactory.performTransaction(() -> {

            // Test that empty components means that all components are considered.
            String[] components = {};
            String[] severities = {};
            List<AlertLog> alerts = alertCrud.retrieve(date2, date6, components, severities);
            assertEquals(6, alerts.size());

            // This component isn't in the date range.
            components = new String[] { "s4" };
            alerts = alertCrud.retrieve(date2, date6, components, severities);
            assertEquals(0, alerts.size());

            // This component has one entry in and one outside of the range.
            components[0] = "s1";
            alerts = alertCrud.retrieve(date2, date6, components, severities);
            assertEquals(1, alerts.size());
            assertEquals("D", alerts.get(0).getAlertData().getProcessName());

            // Specify all components; check sort.
            components = new String[] { "s1", "s2", "s3", "s4" };
            severities = new String[] { Level.ERROR.toString(), Level.ERROR.toString(),
                Level.DEBUG.toString(), Level.INFO.toString(), Level.WARN.toString(),
                Level.ERROR.toString() };
            alerts = alertCrud.retrieve(date1, date7, components, severities);
            assertEquals(5, alerts.size());
            assertEquals("D", alerts.get(0).getAlertData().getProcessName());
            assertEquals("E", alerts.get(1).getAlertData().getProcessName());
            assertEquals("C", alerts.get(2).getAlertData().getProcessName());
            assertEquals("B", alerts.get(3).getAlertData().getProcessName());
            assertEquals("A", alerts.get(4).getAlertData().getProcessName());
            assertEquals(Level.ERROR.toString(), alerts.get(0).getAlertData().getSeverity());

            components = new String[] { "s5", "s6", "s7", "s8" };
            alerts = alertCrud.retrieve(date1, date7, components, severities);
            assertEquals(5, alerts.size());
            assertEquals(Level.ERROR.toString(), alerts.get(0).getAlertData().getSeverity());
            assertEquals(Level.ERROR.toString(), alerts.get(1).getAlertData().getSeverity());
            assertEquals(Level.DEBUG.toString(), alerts.get(2).getAlertData().getSeverity());
            assertEquals(Level.INFO.toString(), alerts.get(3).getAlertData().getSeverity());
            assertEquals(Level.WARN.toString(), alerts.get(4).getAlertData().getSeverity());

            // Check just the SEVERE severity.
            severities = new String[] { Level.ERROR.toString() };
            alerts = alertCrud.retrieve(date1, date7, components, severities);
            assertEquals(2, alerts.size());
            assertEquals(Level.ERROR.toString(), alerts.get(0).getAlertData().getSeverity());
            return null;
        });
    }

    @Test
    public void testRetrieveForPipelineInstance() {
        populateInstancesAndTasks();
        populateObjects();

        DatabaseTransactionFactory.performTransaction(() -> {
            List<AlertLog> logs = alertCrud.retrieveForPipelineInstance(1L);
            assertEquals(4, logs.size());
            return null;
        });
    }

    private void populateObjects() {
        DatabaseTransactionFactory.performTransaction(() -> {
            alertCrud.persist(new AlertLog(new Alert(date7, "s1", 5, "E", "e", 5, "message5")));
            alertCrud.persist(new AlertLog(new Alert(date5, "s1", 4, "D", "d", 4, "message4")));
            alertCrud.persist(new AlertLog(new Alert(date4, "s2", 3, "C", "c", 3, "message3")));
            alertCrud.persist(new AlertLog(new Alert(date3, "s3", 2, "B", "b", 2, "message2")));
            alertCrud.persist(new AlertLog(new Alert(date1, "s4", 1, "A", "a", 1, "message1")));

            alertCrud.persist(new AlertLog(
                new Alert(date5, "s5", 4, "DS", "d", 4, Level.ERROR.toString(), "message4")));
            alertCrud.persist(new AlertLog(
                new Alert(date7, "s5", 5, "EF", "e", 5, Level.ERROR.toString(), "message5")));
            alertCrud.persist(new AlertLog(
                new Alert(date4, "s6", 3, "CD", "c", 3, Level.DEBUG.toString(), "message3")));
            alertCrud.persist(new AlertLog(
                new Alert(date3, "s7", 2, "BI", "b", 2, Level.INFO.toString(), "message2")));
            alertCrud.persist(new AlertLog(
                new Alert(date1, "s8", 1, "AW", "a", 1, Level.WARN.toString(), "message1")));
            return null;
        });
    }

    private void populateInstancesAndTasks() {
        DatabaseTransactionFactory.performTransaction(() -> {

            PipelineInstance instance1 = new PipelineInstance();
            PipelineTask task1 = new PipelineTask();
            task1.setPipelineInstance(instance1);
            PipelineTask task2 = new PipelineTask();
            task2.setPipelineInstance(instance1);

            PipelineInstance instance2 = new PipelineInstance();
            PipelineTask task3 = new PipelineTask();
            task3.setPipelineInstance(instance2);
            PipelineTask task4 = new PipelineTask();
            task4.setPipelineInstance(instance2);
            PipelineTask task5 = new PipelineTask();
            task5.setPipelineInstance(instance2);

            PipelineInstanceCrud instanceCrud = new PipelineInstanceCrud();
            instanceCrud.persist(instance1);
            instanceCrud.persist(instance2);

            PipelineTaskCrud taskCrud = new PipelineTaskCrud();
            taskCrud.persist(task1);
            taskCrud.persist(task2);
            taskCrud.persist(task3);
            taskCrud.persist(task4);
            taskCrud.persist(task5);
            return null;
        });
    }
}
