package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;

/**
 * Performs unit tests for {@link PipelineInstance}.
 *
 * @author Bill Wohler
 */
public class PipelineInstanceTest {

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testHashCodeEquals() {
        PipelineInstance pipelineInstance1 = new PipelineInstance();
        PipelineInstance pipelineInstance2 = new PipelineInstance();
        assertEquals(pipelineInstance1.hashCode(), pipelineInstance2.hashCode());
        assertTrue(pipelineInstance1.equals(pipelineInstance1));
        assertTrue(pipelineInstance1.equals(pipelineInstance2));

        pipelineInstance2.setState(State.PROCESSING);
        assertEquals(pipelineInstance1.hashCode(), pipelineInstance2.hashCode());
        assertTrue(pipelineInstance1.equals(pipelineInstance2));

        pipelineInstance2.getExecutionClock().start();
        assertEquals(pipelineInstance1.hashCode(), pipelineInstance2.hashCode());
        assertTrue(pipelineInstance1.equals(pipelineInstance2));

        pipelineInstance2.setId(42L);
        assertNotEquals(pipelineInstance1.hashCode(), pipelineInstance2.hashCode());
        assertFalse(pipelineInstance1.equals(pipelineInstance2));

        assertFalse(pipelineInstance1.equals(null));
        assertFalse(pipelineInstance1.equals("a string"));
    }

    @Test
    public void testTotalHashCodeEquals() {
        PipelineInstance pipelineInstance1 = new PipelineInstance();
        PipelineInstance pipelineInstance2 = new PipelineInstance();
        assertEquals(pipelineInstance1.totalHashCode(), pipelineInstance2.totalHashCode());
        assertTrue(pipelineInstance1.totalEquals(pipelineInstance1));
        assertTrue(pipelineInstance1.totalEquals(pipelineInstance2));

        pipelineInstance2.setState(State.PROCESSING);
        assertNotEquals(pipelineInstance1.totalHashCode(), pipelineInstance2.totalHashCode());
        assertFalse(pipelineInstance1.totalEquals(pipelineInstance2));

        pipelineInstance2.getExecutionClock().start();
        assertNotEquals(pipelineInstance1.totalHashCode(), pipelineInstance2.totalHashCode());
        assertFalse(pipelineInstance1.totalEquals(pipelineInstance2));

        pipelineInstance2.setId(42L);
        assertNotEquals(pipelineInstance1.totalHashCode(), pipelineInstance2.totalHashCode());
        assertFalse(pipelineInstance1.totalEquals(pipelineInstance2));

        assertFalse(pipelineInstance1.totalEquals(null));
        assertFalse(pipelineInstance1.totalEquals("a string"));
    }
}
