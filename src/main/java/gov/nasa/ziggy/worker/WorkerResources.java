package gov.nasa.ziggy.worker;

import java.io.Serializable;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.HumanReadableHeapSize;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Represents a set of worker resources, specifically the max worker count and Java heap size.
 * <p>
 * Note that any particular instance of {@link WorkerResources} can be one of the following:
 * <ol>
 * <li>The configured resources for a particular {@link PipelineNode} instance, in which case one or
 * both of the values can be null, indicating that the default values should be used.
 * <li>The default values, which were set when the {@link PipelineSupervisor} was instantiated.
 * <li>A composite of the above, in which null values from the node's resources are replaced by the
 * corresponding values from the default resources. This defines the current resources available to
 * the node when defaults are taken into account.
 * </ol>
 * Users should be careful that they know exactly which of these three cases is represented by any
 * particular instance.
 *
 * @author PT
 */
@Entity
@Table(name = "ziggy_WorkerResources")
public class WorkerResources implements Serializable {

    private static final long serialVersionUID = 20250418L;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
        generator = "ziggy_WorkerResources_generator")
    @SequenceGenerator(name = "ziggy_WorkerResources_generator", initialValue = 1,
        sequenceName = "ziggy_WorkerResources_generator_sequence", allocationSize = 1)
    private Long id;

    private int maxWorkerCount;
    private float heapSizeGigabytes;
    private boolean defaultInstance;

    public WorkerResources() {
    }

    public WorkerResources(int maxWorkerCount, float heapSizeGigabytes) {
        this.maxWorkerCount = maxWorkerCount;
        this.heapSizeGigabytes = heapSizeGigabytes;
    }

    public long getId() {
        return id;
    }

    public int getMaxWorkerCount() {
        return maxWorkerCount;
    }

    public void setMaxWorkerCount(int maxWorkerCount) {
        this.maxWorkerCount = maxWorkerCount;
    }

    public float getHeapSizeGigabytes() {
        return heapSizeGigabytes;
    }

    public void setHeapSizeGigabytes(float heapSizeGigabytes) {
        this.heapSizeGigabytes = heapSizeGigabytes;
    }

    public boolean isDefaultInstance() {
        return defaultInstance;
    }

    public void setDefaultInstance(boolean defaultInstance) {
        this.defaultInstance = defaultInstance;
    }

    public HumanReadableHeapSize humanReadableHeapSize() {
        return new HumanReadableHeapSize(getHeapSizeGigabytes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultInstance, heapSizeGigabytes, maxWorkerCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WorkerResources other = (WorkerResources) obj;
        return defaultInstance == other.defaultInstance && Float
            .floatToIntBits(heapSizeGigabytes) == Float.floatToIntBits(other.heapSizeGigabytes)
            && maxWorkerCount == other.maxWorkerCount;
    }
}
