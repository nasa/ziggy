package gov.nasa.ziggy.util.os;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import gov.nasa.ziggy.IntegrationTestCategory;

/**
 * Tests the OS-specific implementation of the {@link ProcInfo} class.
 *
 * @author Forrest Girouard
 */
public class ProcInfoTest {
    @Category(IntegrationTestCategory.class)
    @Test
    public void testChildPids() throws Exception {
        List<Long> childPids = OperatingSystemType.newInstance().getProcInfo(1).getChildPids();

        assertNotNull("childPids null", childPids);
        assertTrue("At least one child", childPids.size() > 0);
    }

    @Category(IntegrationTestCategory.class)
    @Test
    public void testChildPidsNoSuchName() throws Exception {
        List<Long> childPids = OperatingSystemType.newInstance()
            .getProcInfo(1)
            .getChildPids("NoSuchName");

        assertNotNull("childPids null", childPids);
        assertTrue("No children", childPids.size() == 0);
    }

    @Category(IntegrationTestCategory.class)
    @Test
    public void testChildPidsByName() throws Exception {
        OperatingSystemType osType = OperatingSystemType.newInstance();
        String osName = osType.getName();
        String processName;
        if (osName.equals(OperatingSystemType.MAC_OS_X.getName())) {
            processName = "timed";
        } else {
            processName = "sshd";
        }
        List<Long> childPids = osType.getProcInfo(1).getChildPids(processName);

        assertNotNull("childPids null", childPids);
        assertTrue("At least one child", childPids.size() > 0);
    }

    @Test
    public void testParentPid() throws Exception {
        assertEquals(0, OperatingSystemType.newInstance().getProcInfo(1).getParentPid());
    }

    @Test
    public void testPPid() throws Exception {
        ProcInfo procInfo = OperatingSystemType.newInstance()
            .getProcInfo(gov.nasa.ziggy.util.os.ProcessUtils.getPid());
        assertNotEquals("Unexpected parent pid", procInfo.getPid(), procInfo.getParentPid());
    }

    @Test
    public void testOpenFileLimit() throws Exception {
        int limit = OperatingSystemType.newInstance().getProcInfo().getOpenFileLimit();
        assertTrue("Max open files", limit > 0);
    }

    @Test
    public void testMaximumPid() throws Exception {
        long maxPid = OperatingSystemType.newInstance().getProcInfo().getMaximumPid();
        assertTrue("Maximum pid value", maxPid > 0);
    }
}
