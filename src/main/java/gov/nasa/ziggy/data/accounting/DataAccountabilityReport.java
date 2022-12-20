package gov.nasa.ziggy.data.accounting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;

/**
 * Given a set of task ids indicating where data originated from generate the the multi-graph
 * transitive closure of all parent task ids. Subclass this class in order to generate different
 * report formats.
 *
 * @author Sean McCauliff
 */
public class DataAccountabilityReport {
    private static final int DATA_RECEIPT_ID = 0;

    private final Set<Long> initialTaskIds;
    private final PipelineTaskCrud taskCrud;
    private final PipelineTaskRenderer taskRenderer;

    public DataAccountabilityReport(Set<Long> initialTaskIds, PipelineTaskCrud taskCrud,
        PipelineTaskRenderer taskRenderer) {
        this.initialTaskIds = initialTaskIds;
        this.taskCrud = taskCrud;
        this.taskRenderer = taskRenderer;
    }

    public String produceReport() throws IOException {
        Map<Long, Set<Long>> consumerProducer = calculateClosure();
        Map<Long, Set<Long>> producerConsumer = invertMap(consumerProducer);
        Set<Long> roots = findRoots(producerConsumer.keySet(), consumerProducer, producerConsumer);

        return formatReport(producerConsumer, roots);
    }

    /**
     * Calculates the transitive closure of the relation produced(c,p).
     *
     * @param initialTaskIds
     * @param acctCrud
     * @return A {@link Map} from consumer to producer task id. If nothing then this returns an
     * empty map.
     */
    Map<Long, Set<Long>> calculateClosure() {
        Map<Long, Set<Long>> consumerProducer = new HashMap<>();

        // As we expand new producers we needs to explore them as well.
        SortedSet<Long> queue = new TreeSet<>(initialTaskIds);
        // We don't want to explore the same path again.
        Set<Long> visited = new HashSet<>();

        // This does not use a "for" or an iterator since we add more tasks
        // into the queue as we iterate through the queue.
        while (!queue.isEmpty()) {
            long taskId = queue.first();
            queue.remove(taskId);
            visited.add(taskId);

            PipelineTask consumerTask = taskCrud.retrieve(taskId);
            Set<Long> parentTasks = consumerTask.getProducerTaskIds();

            if (parentTasks != null && !parentTasks.isEmpty()) {
                consumerProducer.put(taskId, parentTasks);
            }

            for (long parentTask : parentTasks) {
                if (parentTask == DATA_RECEIPT_ID || visited.contains(parentTask)) {
                    continue;
                }

                queue.add(parentTask);
            }
        }

        return consumerProducer;
    }

    /**
     * @param consumerProducer consumer->producer
     * @return producer->consumer
     */
    Map<Long, Set<Long>> invertMap(Map<Long, Set<Long>> consumerProducer) {
        Map<Long, Set<Long>> producerConsumer = new HashMap<>();

        for (Map.Entry<Long, Set<Long>> entry : consumerProducer.entrySet()) {
            for (long producer : entry.getValue()) {
                if (!producerConsumer.containsKey(producer)) {
                    producerConsumer.put(producer, new HashSet<>());
                }

                producerConsumer.get(producer).add(entry.getKey());
            }
        }

        return producerConsumer;
    }

    /**
     * Finds the ids which are not pointed at by anything. Find all the producers which are not
     * themselves consumers.
     *
     * @return
     */
    Set<Long> findRoots(Set<Long> producers, Map<Long, Set<Long>> consumerProducer,
        Map<Long, Set<Long>> producerConsumer) {
        Set<Long> roots = new HashSet<>();

        /// Handle the case where the initial set of producers may not consume
        // anything.
        Set<Long> producersWithInit = new HashSet<>(producers);
        producersWithInit.addAll(initialTaskIds);

        // Handle the case where the initial task ids are also producers and they
        // are not in a list because they are in a loop.

        for (Long initialTask : initialTaskIds) {
            if (producerConsumer.containsKey(initialTask)) {
                roots.add(initialTask);
            }
        }

        for (Long producer : producersWithInit) {
            Set<Long> parents = consumerProducer.get(producer);
            if ((parents == null) || (parents.size() == 0)) {
                roots.add(producer);
            }
        }

        return roots;
    }

    /**
     * @param producerConsumer producer -&gt; consumer
     * @param roots The top level consumers.
     * @return report
     * @throws IOException
     * @throws PipelineException
     */

    protected String formatReport(Map<Long, Set<Long>> producerConsumer, Set<Long> roots)
        throws IOException {
        List<Long> sortedRoots = new ArrayList<>(roots);
        Collections.sort(sortedRoots);

        StringBuilder bldr = new StringBuilder();

        for (long root : sortedRoots) {
            Set<Long> visited = new HashSet<>();
            printTask(bldr, root, producerConsumer, 0, visited);
        }

        return bldr.toString();
    }

    protected void printTask(Appendable bldr, long taskId, Map<Long, Set<Long>> producerConsumer,
        int level, Set<Long> visited) throws IOException {
        renderLine(bldr, taskId, level);

        Set<Long> consumers = producerConsumer.get(taskId);

        if (consumers == null) {
            return;
        }

        List<Long> sortedConsumers = new ArrayList<>(consumers);
        Collections.sort(sortedConsumers);

        visited.add(taskId);

        for (long consumer : sortedConsumers) {
            if (visited.contains(consumer)) {
                renderLine(bldr, consumer, level + 1);
            } else {
                printTask(bldr, consumer, producerConsumer, level + 1, visited);
            }
        }
    }

    protected void renderLine(Appendable bldr, long taskId, int level) throws IOException {
        for (int i = 0; i < level; i++) {
            bldr.append("    ");
        }
        if (taskId == DATA_RECEIPT_ID) {
            bldr.append(taskRenderer.renderDefaultTask()).append('\n');
        } else {
            PipelineTask task = taskCrud.retrieve(taskId);
            bldr.append(taskRenderer.renderTask(task));
            bldr.append("\n");
        }
    }
}
