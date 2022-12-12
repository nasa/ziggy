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
        List<String> components = alertCrud.retrieveComponents();
        assertEquals(0, components.size());

        populateObjects();

        components = alertCrud.retrieveComponents();

        // Check number of components as well as sort.
        assertEquals(8, components.size());
        assertEquals("s1", components.get(0));
        assertEquals("s2", components.get(1));
        assertEquals("s3", components.get(2));
        assertEquals("s4", components.get(3));
    }

    @Test
    public void testRetrieveSeverities() {
        List<String> severities = alertCrud.retrieveSeverities();
        assertEquals(0, severities.size());

        populateObjects();

        severities = alertCrud.retrieveSeverities();

        // Check number of components as well as sort.
        assertEquals(4, severities.size());
        assertEquals(Level.DEBUG.toString(), severities.get(0));
        assertEquals(Level.ERROR.toString(), severities.get(1));
        assertEquals(Level.INFO.toString(), severities.get(2));
        assertEquals(Level.WARN.toString(), severities.get(3));
    }

    @Test
    public void testCreateRetrieve() throws Exception {
        populateObjects();

        List<AlertLog> alerts = alertCrud.retrieve(date2, date6);
        assertEquals("alerts.size()", 6, alerts.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRetrieveNullStart() {
        alertCrud.retrieve(null, null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testRetrieveNullEnd() {
        alertCrud.retrieve(new Date(), null, null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testRetrieveNullComponent() {
        alertCrud.retrieve(new Date(), new Date(), null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testRetrieveNullSeverity() {
        alertCrud.retrieve(new Date(), new Date(), new String[0], null);
    }

    @Test
    public void testRetrieveAlertsByTaskId() {
        populateObjects();

        List<Long> taskIds = new ArrayList<>();
        taskIds.add(5L);
        taskIds.add(1L);

        List<AlertLog> alerts = alertCrud.retrieveByPipelineTaskIds(taskIds);
        assertEquals(4, alerts.size());
    }

    @Test
    public void testRetrieve() {
        populateObjects();

        // Test that empty components means that all components are considered.
        String[] components = new String[0];
        String[] severities = new String[0];
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
    }

    private void populateObjects() {
        DatabaseTransactionFactory.performTransaction(() -> {
            alertCrud.create(new AlertLog(new Alert(date7, "s1", 5, "E", "e", 105, "message5")));
            alertCrud.create(new AlertLog(new Alert(date5, "s1", 4, "D", "d", 104, "message4")));
            alertCrud.create(new AlertLog(new Alert(date4, "s2", 3, "C", "c", 103, "message3")));
            alertCrud.create(new AlertLog(new Alert(date3, "s3", 2, "B", "b", 102, "message2")));
            alertCrud.create(new AlertLog(new Alert(date1, "s4", 1, "A", "a", 101, "message1")));

            alertCrud.create(new AlertLog(
                new Alert(date5, "s5", 4, "DS", "d", 104, Level.ERROR.toString(), "message4")));
            alertCrud.create(new AlertLog(
                new Alert(date7, "s5", 5, "EF", "e", 105, Level.ERROR.toString(), "message5")));
            alertCrud.create(new AlertLog(
                new Alert(date4, "s6", 3, "CD", "c", 103, Level.DEBUG.toString(), "message3")));
            alertCrud.create(new AlertLog(
                new Alert(date3, "s7", 2, "BI", "b", 102, Level.INFO.toString(), "message2")));
            alertCrud.create(new AlertLog(
                new Alert(date1, "s8", 1, "AW", "a", 101, Level.WARN.toString(), "message1")));
            return null;
        });
    }
}
