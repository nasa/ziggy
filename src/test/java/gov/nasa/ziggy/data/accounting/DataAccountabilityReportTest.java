package gov.nasa.ziggy.data.accounting;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.uow.UnitOfWork;

/**
 * @author Sean McCauliff
 */
public class DataAccountabilityReportTest {
    private final StubPipelineTaskCrud pipelineTaskCrud = new StubPipelineTaskCrud();
    private final ReflectionEquals reflectionEquals = new ReflectionEquals();
    private final SimpleTaskRenderer taskRenderer = new SimpleTaskRenderer();
    private static Map<Long, Set<Long>> consumerProducer = new HashMap<>();

    @Test
    public void emptyReport() throws Exception {
        DataAccountabilityReport report = new DataAccountabilityReport(new HashSet<>(),
            pipelineTaskCrud, taskRenderer);

        String reportStr = report.produceReport();
        assertEquals("", reportStr);
    }

    /**
     * The initial set of task ids do not depend on anything.
     *
     * @throws Exception
     */
    @Test
    public void noProducers() throws Exception {
        Set<Long> init = new HashSet<>();
        init.add(0L);
        init.add(1L);

        DataAccountabilityReport report = new DataAccountabilityReport(init, pipelineTaskCrud,
            taskRenderer);
        reflectionEquals.assertEquals(new HashMap<Long, Set<Long>>(), report.calculateClosure());
        reflectionEquals.assertEquals(init,
            report.findRoots(new HashSet<Long>(), new HashMap<>(), new HashMap<>()));

        String expectedReport = "Data Receipt\nStub taskId = 1\n";
        assertEquals(expectedReport, report.produceReport());
    }

    /**
     * <pre>
     *  consumer 1 - > producer0
     *  consumer 2 -> producer 3
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void mutuallyExclusiveProducers() throws Exception {
        Set<Long> init = new HashSet<>();
        // init.add(0L);
        init.add(1L);
        init.add(2L);

        consumerProducer = new HashMap<>();
        consumerProducer.put(1L, Collections.singleton(0L));
        consumerProducer.put(2L, Collections.singleton(3L));

        Map<Long, Set<Long>> expectedProducerConsumer = new HashMap<>();
        expectedProducerConsumer.put(0L, Collections.singleton(1L));
        expectedProducerConsumer.put(3L, Collections.singleton(2L));

        Set<Long> expectedRoots = new HashSet<>();
        expectedRoots.add(0L);
        expectedRoots.add(3L);

        DataAccountabilityReport report = new DataAccountabilityReport(init, pipelineTaskCrud,
            taskRenderer);

        reflectionEquals.assertEquals(consumerProducer, report.calculateClosure());
        Map<Long, Set<Long>> producerConsumer = report.invertMap(consumerProducer);
        reflectionEquals.assertEquals(expectedProducerConsumer, producerConsumer);
        Set<Long> roots = report.findRoots(producerConsumer.keySet(), consumerProducer,
            producerConsumer);
        reflectionEquals.assertEquals(expectedRoots, roots);

        String expectedReport = "Data Receipt\n    Stub taskId = 1\nStub taskId = 3\n    Stub taskId = 2\n";
        assertEquals(expectedReport, report.produceReport());
    }

    /**
     * <pre>
     *                    6
     *                   /  \
     *                 5     4
     *               /  \      / \
     *             3   2    1   0
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void pyramid() throws Exception {
        Set<Long> init = new HashSet<>();
        init.addAll(Arrays.asList(new Long[] { 3L, 2L, 1L, 0L }));

        consumerProducer = new HashMap<>();
        consumerProducer.put(3L, Collections.singleton(5L));
        consumerProducer.put(2L, Collections.singleton(5L));
        consumerProducer.put(1L, Collections.singleton(4L));
        consumerProducer.put(0L, Collections.singleton(4L));
        consumerProducer.put(5L, Collections.singleton(6L));
        consumerProducer.put(4L, Collections.singleton(6L));

        DataAccountabilityReport report = new DataAccountabilityReport(init, pipelineTaskCrud,
            taskRenderer);
        String expectedReport = "Stub taskId = 6\n    Stub taskId = 4\n"
            + "        Data Receipt\n        Stub taskId = 1\n"
            + "    Stub taskId = 5\n        Stub taskId = 2\n        Stub taskId = 3\n";

        assertEquals(expectedReport, report.produceReport());
    }

    /**
     * <pre>
     *        3   2   1   0
     *         \   /     \   /
     *          5         4
     *            \      /
     *               6
     * </pre>
     */
    @Test
    public void invertedPyramid() throws Exception {
        Set<Long> init = Collections.singleton(6L);

        consumerProducer = new HashMap<>();
        consumerProducer.put(6L, Sets.newHashSet(new Long[] { 5L, 4L }));
        consumerProducer.put(5L, Sets.newHashSet(new Long[] { 3L, 2L }));
        consumerProducer.put(4L, Sets.newHashSet(new Long[] { 1L, 0L }));

        DataAccountabilityReport report = new DataAccountabilityReport(init, pipelineTaskCrud,
            taskRenderer);

        String expectedReport = "Data Receipt\n    Stub taskId = 4\n        Stub taskId = 6\n"
            + "Stub taskId = 1\n    Stub taskId = 4\n        Stub taskId = 6\n"
            + "Stub taskId = 2\n    Stub taskId = 5\n        Stub taskId = 6\n"
            + "Stub taskId = 3\n    Stub taskId = 5\n        Stub taskId = 6\n";

        assertEquals(expectedReport, report.produceReport());
    }

    /**
     * <pre>
     * Circular
     *            1
     *           ^ \
     *           /    v      X->Y ( X produces Y)
     *          4    2
     *          ^   /
     *           \   v
     *             3
     * </pre>
     */
    @Test
    public void circular() throws Exception {
        Set<Long> init = Collections.singleton(1L);
        consumerProducer = new HashMap<>();
        consumerProducer.put(2L, Collections.singleton(1L));
        consumerProducer.put(3L, Collections.singleton(2L));
        consumerProducer.put(4L, Collections.singleton(3L));
        consumerProducer.put(1L, Collections.singleton(4L));

        Map<Long, Set<Long>> producerConsumer = new HashMap<>();
        producerConsumer.put(1L, Collections.singleton(2L));
        producerConsumer.put(2L, Collections.singleton(3L));
        producerConsumer.put(3L, Collections.singleton(4L));
        producerConsumer.put(4L, Collections.singleton(1L));

        DataAccountabilityReport report = new DataAccountabilityReport(init, pipelineTaskCrud,
            taskRenderer);

        reflectionEquals.assertEquals(consumerProducer, report.calculateClosure());
        reflectionEquals.assertEquals(producerConsumer, report.invertMap(consumerProducer));
        reflectionEquals.assertEquals(init,
            report.findRoots(init, consumerProducer, producerConsumer));

        String expectedReport = "Stub taskId = 1\n    Stub taskId = 2\n        Stub taskId = 3\n"
            + "            Stub taskId = 4\n                Stub taskId = 1\n";
        String actualReport = report.produceReport();
        assertEquals(actualReport + expectedReport, expectedReport, actualReport);
    }

    /**
     * <pre>
     * Cycle
     *
     *       1
     *         \
     *           2
     *           | \ 4
     *           | /
     *           3
     * </pre>
     */
    @Test
    public void cycle() throws Exception {
        Set<Long> init = Collections.singleton(3L);

        consumerProducer = new HashMap<>();
        consumerProducer.put(3L, Sets.newHashSet(new Long[] { 2L, 4L }));
        consumerProducer.put(4L, Collections.singleton(2L));
        consumerProducer.put(2L, Collections.singleton(1L));

        DataAccountabilityReport report = new DataAccountabilityReport(init, pipelineTaskCrud,
            taskRenderer);

        String expectedReport = "Stub taskId = 1\n    Stub taskId = 2\n        Stub taskId = 3\n"
            + "        Stub taskId = 4\n            Stub taskId = 3\n";
        String actualReport = report.produceReport();

        assertEquals(actualReport + expectedReport, expectedReport, actualReport);
    }

    private static class StubPipelineTaskCrud extends PipelineTaskCrud {
        public StubPipelineTaskCrud() {
            super(null);
        }

        @Override
        public PipelineTask retrieve(final long pipelineTaskId) {
            PipelineTask task = new PipelineTask();
            task.setId(pipelineTaskId);
            task.setProducerTaskIds(consumerProducer.get(pipelineTaskId));

            UnitOfWork uowt = new UnitOfWork();
            uowt.addParameter(new TypedParameter(UnitOfWork.BRIEF_STATE_PARAMETER_NAME,
                "Stub taskId = " + pipelineTaskId, ZiggyDataType.ZIGGY_STRING));

            BeanWrapper<UnitOfWork> bwUow;
            try {
                bwUow = new BeanWrapper<>(uowt);
            } catch (PipelineException e) {
                throw new IllegalStateException("Can't instantiate UowTask.", e);
            }
            task.setUowTask(bwUow);

            return task;
        }
    }

}
