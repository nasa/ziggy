package gov.nasa.ziggy.module.io.matlab;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import gov.nasa.ziggy.util.os.OperatingSystemType;

public class MatlabUtilsTest {

    @Test
    public void testLinuxMcrPath() {
        MatlabUtils.setOsType(OperatingSystemType.LINUX);
        String mPath = MatlabUtils.mcrPaths("/path/to/mcr/v93");
        String mPathExpect = """
            /path/to/mcr/v93/runtime/glnxa64:\
            /path/to/mcr/v93/bin/glnxa64:\
            /path/to/mcr/v93/sys/os/glnxa64:\
            /path/to/mcr/v93/sys/opengl/lib/glnxa64""";
        assertEquals(mPathExpect, mPath);
    }

    @Test
    public void testOsXIntelMcrPath() {
        MatlabUtils.setOsType(OperatingSystemType.MAC_OS_X);
        MatlabUtils.setArchitecture("x86_64");
        String mPath = MatlabUtils.mcrPaths("/path/to/mcr/v93");
        String mPathExpect = """
            /path/to/mcr/v93/runtime/maci64:\
            /path/to/mcr/v93/bin/maci64:\
            /path/to/mcr/v93/sys/os/maci64""";
        assertEquals(mPathExpect, mPath);
    }

    @Test
    public void testOsXM1McrPath() {
        MatlabUtils.setOsType(OperatingSystemType.MAC_OS_X);
        MatlabUtils.setArchitecture("aarch");
        String mPath = MatlabUtils.mcrPaths("/path/to/mcr/v93");
        String mPathExpect = """
            /path/to/mcr/v93/runtime/maca64:\
            /path/to/mcr/v93/bin/maca64:\
            /path/to/mcr/v93/sys/os/maca64""";
        assertEquals(mPathExpect, mPath);
    }
}
