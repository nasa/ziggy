package gov.nasa.ziggy.services.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import gov.nasa.ziggy.util.SystemProxy;

/**
 * Performs unit tests for {@link ZiggyEvent}.
 *
 * @author Bill Wohler
 */
public class ZiggyEventTest {

    private static final String EVENT_HANDLER_NAME = "eventHandlerName";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final int PIPELINE_INSTANCE = 1;
    private static final Set<String> EVENT_LABELS = new TreeSet<>(Set.of("label1", "label2"));
    private static final long TIME1 = 1000;
    private ZiggyEvent ziggyEvent;

    @Before
    public void setUp() {
        SystemProxy.setUserTime(TIME1);
        ziggyEvent = new ZiggyEvent(EVENT_HANDLER_NAME, PIPELINE_NAME, PIPELINE_INSTANCE,
            EVENT_LABELS);
    }

    @Test
    public void testGetId() {
        assertNull(ziggyEvent.getId());
    }

    @Test
    public void testGetEventHandlerName() {
        assertEquals(EVENT_HANDLER_NAME, ziggyEvent.getEventHandlerName());
    }

    @Test
    public void testGetPipelineName() {
        assertEquals(PIPELINE_NAME, ziggyEvent.getPipelineName());
    }

    @Test
    public void testGetEventTime() {
        assertEquals(new Date(TIME1), ziggyEvent.getEventTime());
    }

    @Test
    public void testGetPipelineInstanceId() {
        assertEquals(PIPELINE_INSTANCE, ziggyEvent.getPipelineInstanceId());
    }

    @Test
    public void testGetEventLabels() {
        assertEquals(EVENT_LABELS, ziggyEvent.getEventLabels());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        ZiggyEvent ziggyEvent1 = ziggyEvent;
        SystemProxy.setUserTime(TIME1);
        ZiggyEvent ziggyEvent2 = new ZiggyEvent(EVENT_HANDLER_NAME, PIPELINE_NAME,
            PIPELINE_INSTANCE, EVENT_LABELS);

        assertEquals(ziggyEvent1.hashCode(), ziggyEvent2.hashCode());
        assertTrue(ziggyEvent1.equals(ziggyEvent1));
        assertTrue(ziggyEvent1.equals(ziggyEvent2));

        assertFalse(ziggyEvent1.equals(null));
        assertFalse(ziggyEvent1.equals("a string"));
    }

    @Test
    public void testToString() {
        assertEquals("eventHandlerName=" + EVENT_HANDLER_NAME + ", pipelineName=" + PIPELINE_NAME
            + ", eventTime=" + new Date(TIME1) + ", pipelineInstanceId=" + PIPELINE_INSTANCE
            + ", eventLabels=" + EVENT_LABELS, ziggyEvent.toString());
    }
}
