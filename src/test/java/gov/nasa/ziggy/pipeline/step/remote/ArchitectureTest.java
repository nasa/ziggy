package gov.nasa.ziggy.pipeline.step.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** Unit tests for the {@link Architecture} class. */
public class ArchitectureTest {

    private Architecture architecture = Mockito.spy(Architecture.class);

    @Before
    public void setUp() {
        Mockito.doReturn("name").when(architecture).getName();
        Mockito.doReturn("description").when(architecture).getDescription();
        Mockito.doReturn(32).when(architecture).getCores();
        Mockito.doReturn(128).when(architecture).getRamGigabytes();
        Mockito.doReturn(1.0F).when(architecture).getCost();
        Mockito.doReturn(10.0F).when(architecture).getBandwidthGbps();
    }

    @Test
    public void testHasSufficientRam() {
        assertFalse(architecture.hasSufficientRam(129));
        assertTrue(architecture.hasSufficientRam(127));
    }

    @Test
    public void testGigsPerCore() {
        assertEquals(4.0, architecture.gigsPerCore(), 1e-6);
    }
}
