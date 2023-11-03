package gov.nasa.ziggy.module.remote.nas;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_NASA_DIRECTORATE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.remote.RemoteNodeDescriptor;
import gov.nasa.ziggy.services.process.ExternalProcess;

/**
 * Provides unit tests for {@link NasQueueTimeMetrics} class.
 *
 * @author PT
 */
public class NasQueueTimeMetricsTest {

    private NasQueueTimeMetrics instance;
    private ExternalProcess externalProcess;
    private static final String QS_MOCK_OUTPUT_FILE = TEST_DATA.resolve("NasQueueTimeMetrics")
        .resolve("qs.csv")
        .toString();

    @Rule
    public ZiggyPropertyRule nasaDirectoratePropertyRule = new ZiggyPropertyRule(
        REMOTE_NASA_DIRECTORATE, (String) null);

    @Before
    public void setup() {
        externalProcess = mock(ExternalProcess.class);
        instance = spy(NasQueueTimeMetrics.class);
        Mockito.doReturn(externalProcess)
            .when(instance)
            .externalProcess(ArgumentMatchers.any(String.class));
        NasQueueTimeMetrics.setSingletonInstance(instance);
    }

    @Test
    public void testArmdMetrics() {
        System.setProperty(REMOTE_NASA_DIRECTORATE.property(), "ARMD");
        instance.populate(QS_MOCK_OUTPUT_FILE);
        testValues(RemoteNodeDescriptor.SANDY_BRIDGE, 53.8, 1.0);
        testValues(RemoteNodeDescriptor.IVY_BRIDGE, 197.5, 122.4);
        testValues(RemoteNodeDescriptor.HASWELL, 25.7, 3.1);
        testValues(RemoteNodeDescriptor.BROADWELL, 62.6, 4.7);
        testValues(RemoteNodeDescriptor.SKYLAKE, 505.5, 122.5);
        testValues(RemoteNodeDescriptor.CASCADE_LAKE, 239.0, 4.0);
        testValues(RemoteNodeDescriptor.ROME, 98.0, 62.8);
    }

    @Test
    public void testHeomdMetrics() {
        System.setProperty(REMOTE_NASA_DIRECTORATE.property(), "HEOMD");
        instance.populate(QS_MOCK_OUTPUT_FILE);
        testValues(RemoteNodeDescriptor.SANDY_BRIDGE, 278.4, 2.0);
        testValues(RemoteNodeDescriptor.IVY_BRIDGE, 218.9, 2.6);
        testValues(RemoteNodeDescriptor.HASWELL, 228.7, 5.0);
        testValues(RemoteNodeDescriptor.BROADWELL, 158.3, 2.3);
        testValues(RemoteNodeDescriptor.SKYLAKE, 83.1, 4.9);
        testValues(RemoteNodeDescriptor.CASCADE_LAKE, 416.9, 4.9);
        testValues(RemoteNodeDescriptor.ROME, 189.2, 1.6);
    }

    @Test
    public void testSmdMetrics() {
        System.setProperty(REMOTE_NASA_DIRECTORATE.property(), "SMD");
        instance.populate(QS_MOCK_OUTPUT_FILE);
        testValues(RemoteNodeDescriptor.SANDY_BRIDGE, 175.7, 15.6);
        testValues(RemoteNodeDescriptor.IVY_BRIDGE, 199.8, 31.7);
        testValues(RemoteNodeDescriptor.HASWELL, 822.5, 13.2);
        testValues(RemoteNodeDescriptor.BROADWELL, 241.2, 21.1);
        testValues(RemoteNodeDescriptor.SKYLAKE, 165.0, 10.4);
        testValues(RemoteNodeDescriptor.CASCADE_LAKE, 284.9, 4.7);
        testValues(RemoteNodeDescriptor.ROME, 1423.2, 4.4);
    }

    @Test
    public void testDefaultMetrics() {
        instance.populate(QS_MOCK_OUTPUT_FILE);
        testValues(RemoteNodeDescriptor.SANDY_BRIDGE, 175.7, 15.6);
        testValues(RemoteNodeDescriptor.IVY_BRIDGE, 199.8, 31.7);
        testValues(RemoteNodeDescriptor.HASWELL, 822.5, 13.2);
        testValues(RemoteNodeDescriptor.BROADWELL, 241.2, 21.1);
        testValues(RemoteNodeDescriptor.SKYLAKE, 165.0, 10.4);
        testValues(RemoteNodeDescriptor.CASCADE_LAKE, 284.9, 4.7);
        testValues(RemoteNodeDescriptor.ROME, 1423.2, 4.4);
    }

    private void testValues(RemoteNodeDescriptor descriptor, double runout, double expansion) {
        assertEquals(runout, NasQueueTimeMetrics.queueDepth(descriptor), 1e-3);
        assertEquals(expansion, NasQueueTimeMetrics.queueTime(descriptor), 1e-3);
    }
}
