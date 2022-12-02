package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.RemoteJob.RemoteJobQstatInfo;

/**
 * Unit tests for the {@link RemoteJob} and {@link RemoteJobQstatInfo} classes.
 *
 * @author PT
 */
public class RemoteJobTest {

    @Test
    public void testCostEstimate() {

        RemoteJobQstatInfo info = new RemoteJobQstatInfo();
        info.setModel("has");
        info.setNodes(2);
        info.setWallTime("100:00:00");
        assertEquals(160.0, info.costEstimate(), 1e-9);

        info.setModel("bro_ele");
        assertEquals(200.0, info.costEstimate(), 1e-9);

        info.setNodes(0);
        info.setModel(null);
        info.setWallTime(null);
        assertEquals(0, info.costEstimate(), 1e-9);

        info.setNodes(1);
        info.setModel("bro_ele");
        assertEquals(0, info.costEstimate(), 1e-9);

    }
}
