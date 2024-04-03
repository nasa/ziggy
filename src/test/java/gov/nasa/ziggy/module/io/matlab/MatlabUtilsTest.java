package gov.nasa.ziggy.module.io.matlab;

import static gov.nasa.ziggy.services.config.PropertyName.ARCHITECTURE;
import static gov.nasa.ziggy.services.config.PropertyName.OPERATING_SYSTEM;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;

public class MatlabUtilsTest {

    @Rule
    public ZiggyPropertyRule osName = new ZiggyPropertyRule(OPERATING_SYSTEM, "Linux");

    @Rule
    public ZiggyPropertyRule architecture = new ZiggyPropertyRule(ARCHITECTURE, (String) null);

    @Test
    public void testLinuxMcrPath() {
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
        osName.setValue("Mac OS X");
        architecture.setValue("x86_64");
        String mPath = MatlabUtils.mcrPaths("/path/to/mcr/v93");
        String mPathExpect = """
            /path/to/mcr/v93/runtime/maci64:\
            /path/to/mcr/v93/bin/maci64:\
            /path/to/mcr/v93/sys/os/maci64""";
        assertEquals(mPathExpect, mPath);
    }

    @Test
    public void testOsXM1McrPath() {
        osName.setValue("Mac OS X");
        architecture.setValue("aarch");
        String mPath = MatlabUtils.mcrPaths("/path/to/mcr/v93");
        String mPathExpect = """
            /path/to/mcr/v93/runtime/maca64:\
            /path/to/mcr/v93/bin/maca64:\
            /path/to/mcr/v93/sys/os/maca64""";
        assertEquals(mPathExpect, mPath);
    }
}
