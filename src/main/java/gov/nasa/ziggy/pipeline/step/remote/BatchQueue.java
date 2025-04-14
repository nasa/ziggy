package gov.nasa.ziggy.pipeline.step.remote;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import gov.nasa.ziggy.ui.pipeline.RemoteExecutionDialog;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlTransient;

/** A batch queue on a remote system. */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_batchQueue")
public class BatchQueue {

    @XmlTransient
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_Architecture_generator")
    @SequenceGenerator(name = "ziggy_batchQueue_generator", initialValue = 1,
        sequenceName = "ziggy_batchQueue_sequence", allocationSize = 1)
    private Long id;

    /** Name used by the batch system. */
    @XmlAttribute(required = true)
    private String name;

    /** More descriptive name used by {@link RemoteExecutionDialog}. */
    @XmlAttribute(required = true)
    private String description;

    /** Wall time limit for this queue. */
    @XmlAttribute(required = false)
    private Float maxWallTimeHours;

    /** Max nodes that can be selected on this queue. */
    @XmlAttribute(required = false)
    private Integer maxNodes;

    /** Indicates a private queue. */
    @XmlAttribute(required = false)
    private Boolean reserved;

    /** Indicates that {@link RemoteExecutionDialog} can select this queue during optimization. */
    @XmlAttribute(required = false)
    private Boolean autoSelectable;

    public BatchQueue() {
    }

    public void updateFrom(BatchQueue importedBatchQueue) {
        description = importedBatchQueue.description;
        maxWallTimeHours = importedBatchQueue.maxWallTimeHours;
        maxNodes = importedBatchQueue.maxNodes;
        reserved = importedBatchQueue.reserved;
        autoSelectable = importedBatchQueue.autoSelectable;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public float getMaxWallTimeHours() {
        return maxWallTimeHours != null ? maxWallTimeHours : Float.MAX_VALUE;
    }

    public int getMaxNodes() {
        return maxNodes != null ? maxNodes : Integer.MAX_VALUE;
    }

    public boolean isReserved() {
        return reserved != null ? reserved : false;
    }

    public boolean isAutoSelectable() {
        return autoSelectable != null ? autoSelectable : true;
    }

    /**
     * Returns the subset of batch queues out of a {@link List} that are auto-selectable. The result
     * is sorted from smallest to largest value of the max wall time for the queue.
     */
    public static List<BatchQueue> autoSelectableBatchQueues(List<BatchQueue> batchQueues) {
        return batchQueues.stream()
            .filter(BatchQueue::isAutoSelectable)
            .sorted(BatchQueue::compareByMaxWallTime)
            .collect(Collectors.toList());
    }

    /**
     * Generates a new instance of a reserved {@link BatchQueue} with the name of the instance set
     * to the reserved queue name.
     */
    public static BatchQueue reservedBatchQueueWithQueueName(BatchQueue queue,
        String reservedQueueName) {
        if (!queue.isReserved()) {
            throw new IllegalArgumentException(
                "Queue " + queue.getName() + " is not a reserved queue");
        }
        BatchQueue reservedQueue = new BatchQueue();
        reservedQueue.description = queue.getDescription();
        reservedQueue.maxWallTimeHours = queue.getMaxWallTimeHours();
        reservedQueue.maxNodes = queue.getMaxNodes();
        reservedQueue.autoSelectable = queue.isAutoSelectable();
        reservedQueue.reserved = true;
        reservedQueue.name = reservedQueueName;
        return reservedQueue;
    }

    private static int compareByMaxWallTime(BatchQueue q1, BatchQueue q2) {
        return (int) Math.signum(q1.getMaxWallTimeHours() - q2.getMaxWallTimeHours());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BatchQueue other = (BatchQueue) obj;
        return Objects.equals(name, other.name);
    }
}
