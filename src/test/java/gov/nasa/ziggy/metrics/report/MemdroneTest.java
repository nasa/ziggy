package gov.nasa.ziggy.metrics.report;

import static gov.nasa.ziggy.services.config.PropertyNames.RESULTS_DIR_PROP_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.util.Iso8601Formatter;

public class MemdroneTest {

    private static final String BINARY_NAME_1 = "cal";
    private static final String BINARY_NAME_2 = "pa";
    private static final long INSTANCE_ID_1 = 15L;
    private static final long INSTANCE_ID_2 = 16L;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule pipelineResultsDirPropertyRule = new ZiggyPropertyRule(
        RESULTS_DIR_PROP_NAME, directoryRule);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(pipelineResultsDirPropertyRule);

    @After
    public void teardown() throws IOException {
    }

    @Test
    public void testMemdronePath() throws IOException, InterruptedException {

        Memdrone memdrone = new Memdrone(BINARY_NAME_1, INSTANCE_ID_1);
        Path memRootPath = directoryRule.directory().resolve("logs").resolve("memdrone");
        assertFalse(Files.exists(memRootPath));

        Date d1 = new Date();
        Date d2 = new Date(d1.getTime() + 2000);
        Date d3 = new Date(d2.getTime() + 2000);
        // Create a file with the first binary name and instance
        memdrone.setDate(d1);
        Path p1 = memdrone.createNewMemdronePath();
        assertTrue(Files.isDirectory(memRootPath));
        assertTrue(Files.exists(p1));
        assertTrue(Files.isDirectory(p1));
        assertTrue(p1.toAbsolutePath().startsWith(memRootPath.toAbsolutePath()));
        String name1 = p1.getFileName().toString();
        String dateString1 = Iso8601Formatter.dateTimeLocalFormatter().format(d1);
        assertEquals("cal-15-" + dateString1, name1);

        // create a PA file and a CAL file at a later date
        memdrone.setDate(d2);
        Path p2 = memdrone.createNewMemdronePath();
        memdrone = new Memdrone(BINARY_NAME_2, INSTANCE_ID_1);
        memdrone.setDate(d2);
        Path p3 = memdrone.createNewMemdronePath();
        String name3 = p3.getFileName().toString();
        String dateString2 = Iso8601Formatter.dateTimeLocalFormatter().format(d2);
        assertEquals("pa-15-" + dateString2, name3);

        // pcreate another PA file and another CAL file, but with a different instance ID
        memdrone = new Memdrone(BINARY_NAME_1, INSTANCE_ID_2);
        memdrone.setDate(d3);
        memdrone.createNewMemdronePath();
        memdrone = new Memdrone(BINARY_NAME_2, INSTANCE_ID_2);
        memdrone.setDate(d3);
        memdrone.createNewMemdronePath();

        // Now get the most recent CAL directory in instance 15
        memdrone = new Memdrone(BINARY_NAME_1, INSTANCE_ID_1);

        Path pLatest = memdrone.latestMemdronePath();
        assertEquals(p2.getFileName().toString(), pLatest.getFileName().toString());

    }
}
