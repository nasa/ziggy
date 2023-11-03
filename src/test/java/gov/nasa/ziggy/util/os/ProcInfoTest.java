package gov.nasa.ziggy.util.os;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.IntegrationTestCategory;

/**
 * Tests the OS-specific implementation of the {@link ProcInfo} class.
 *
 * @author Forrest Girouard
 */
@Category(IntegrationTestCategory.class)
public class ProcInfoTest {
    private static final Logger log = LoggerFactory.getLogger(ProcInfoTest.class);

    @Test
    public void testChildPids() throws Exception {
        List<Long> childPids = OperatingSystemType.getInstance().getProcInfo(1).getChildPids();

        assertNotNull("childPids null", childPids);

        log.info("found " + childPids.size() + " children");

        assertTrue("At least one child", childPids.size() > 0);
    }

    @Test
    public void testChildPidsNoSuchName() throws Exception {
        List<Long> childPids = OperatingSystemType.getInstance()
            .getProcInfo(1)
            .getChildPids("NoSuchName");

        assertNotNull("childPids null", childPids);

        log.info("found " + childPids.size() + " children");

        assertTrue("No children", childPids.size() == 0);
    }

    @Test
    public void testChildPidsByName() throws Exception {
        OperatingSystemType osType = OperatingSystemType.getInstance();
        String osName = osType.getName();
        String processName;
        if (osName.equals(OperatingSystemType.MAC_OS_X.getName())) {
            processName = "timed";
        } else {
            processName = "sshd";
        }
        List<Long> childPids = osType.getProcInfo(1).getChildPids(processName);

        assertNotNull("childPids null", childPids);

        log.info("found " + childPids.size() + " children");

        assertTrue("At least one child", childPids.size() > 0);
    }

    @Test
    public void testParentPid() throws Exception {
        long parentPid = OperatingSystemType.getInstance().getProcInfo(1).getParentPid();

        log.info(String.format("found parent pid of %d for pid %d", parentPid, 1));

        assertTrue("unexpected parent pid", parentPid == 0);
    }

    @Test
    public void testPPid() throws Exception {
        ProcInfo procInfo = OperatingSystemType.getInstance()
            .getProcInfo(gov.nasa.ziggy.util.os.ProcessUtils.getPid());

        log.info(String.format("found parent pid of %d for pid %d", procInfo.getParentPid(),
            procInfo.getPid()));

        assertTrue("unexpected parent pid", procInfo.getPid() != procInfo.getParentPid());
    }

    @Test
    public void testOpenFileLimit() throws Exception {
        int limit = OperatingSystemType.getInstance().getProcInfo().getOpenFileLimit();

        log.info("max open file limit is " + limit);

        assertTrue("max open files", limit > 0);
    }

    @Test
    public void testMaximumPid() throws Exception {
        long maxPid = OperatingSystemType.getInstance().getProcInfo().getMaximumPid();

        log.info("maximum pid value is " + maxPid);

        assertTrue("maximum pid vale", maxPid > 0);
    }
}
