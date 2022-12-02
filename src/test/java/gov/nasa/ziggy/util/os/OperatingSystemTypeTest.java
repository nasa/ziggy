package gov.nasa.ziggy.util.os;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * @author Miles Cote
 */
public class OperatingSystemTypeTest {
    private final OperatingSystemType operatingSystemType = OperatingSystemType.getInstance();

    @Test
    public void testGetName() {
        assertEquals(operatingSystemType == OperatingSystemType.LINUX ? "Linux" : "Darwin",
            operatingSystemType.getName());
    }

    @Test
    public void testGetArchDataModel() {
        assertNotNull(operatingSystemType.getArchDataModel());
    }

    @Test
    public void testGetSharedObjectPathEnvVar() {
        assertEquals(operatingSystemType == OperatingSystemType.LINUX ? "LD_LIBRARY_PATH"
            : "DYLD_LIBRARY_PATH", operatingSystemType.getSharedObjectPathEnvVar());
    }

    @Test
    public void testGetCpuInfo() throws Exception {
        assertEquals(operatingSystemType == OperatingSystemType.LINUX ? LinuxCpuInfo.class
            : MacOSXCpuInfo.class, operatingSystemType.getCpuInfo().getClass());
    }

    @Test
    public void testGetMemInfo() throws Exception {
        assertEquals(operatingSystemType == OperatingSystemType.LINUX ? LinuxMemInfo.class
            : MacOSXMemInfo.class, operatingSystemType.getMemInfo().getClass());
    }

    @Test
    public void testGetProcInfo() throws Exception {
        assertEquals(operatingSystemType == OperatingSystemType.LINUX ? LinuxProcInfo.class
            : MacOSXProcInfo.class, operatingSystemType.getProcInfo().getClass());
    }

    @Test
    public void testGetProcInfoWithPid() throws Exception {
        assertEquals(operatingSystemType == OperatingSystemType.LINUX ? LinuxProcInfo.class
            : MacOSXProcInfo.class, operatingSystemType.getProcInfo(1).getClass());
    }

    @Test
    public void testByName() {
        assertEquals(operatingSystemType,
            OperatingSystemType.byName(operatingSystemType.getName()));
    }

    @Test
    public void testByNameWithEmptyName() {
        assertEquals(OperatingSystemType.DEFAULT, OperatingSystemType.byName(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByNameWithNullName() {
        OperatingSystemType.byName(null);
    }

    @Test
    public void testGetInstance() {
        assertNotNull(OperatingSystemType.getInstance());
    }
}
