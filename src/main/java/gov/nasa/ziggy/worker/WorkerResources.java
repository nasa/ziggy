package gov.nasa.ziggy.worker;

import java.io.Serializable;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.services.messages.WorkerResourcesMessage;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.util.HumanReadableHeapSize;

/**
 * Represents a set of worker resources, specifically the max worker count and Java heap size.
 * <p>
 * Note that any particular instance of {@link WorkerResources} can be one of the following:
 * <ol>
 * <li>The configured resources for a particular {@link PipelineDefinitionNode} instance, in which
 * case one or both of the values can be null, indicating that the default values should be used.
 * <li>The default values, which were set when the {@link PipelineSupervisor} was instantiated.
 * <li>A composite of the above, in which null values from the node's resources are replaced by the
 * corresponding values from the default resources. This defines the current resources available to
 * the node when defaults are taken into account.
 * </ol>
 * Users should be careful that they know exactly which of these three cases is represented by any
 * particular instance.
 * <p>
 * Note that the {@link WorkerResourcesMessage} is incapable of transporting a non-default instance
 * of {@link WorkerResources} if the value of any resource is null.
 *
 * @author PT
 */
public class WorkerResources implements Serializable {

    private static final long serialVersionUID = 20231204L;
    private final Integer maxWorkerCount;
    private final Integer heapSizeMb;

    public WorkerResources(Integer maxWorkerCount, Integer heapSizeMb) {
        this.maxWorkerCount = maxWorkerCount;
        this.heapSizeMb = heapSizeMb;
    }

    public Integer getMaxWorkerCount() {
        return maxWorkerCount;
    }

    public Integer getHeapSizeMb() {
        return heapSizeMb;
    }

    public HumanReadableHeapSize humanReadableHeapSize() {
        return new HumanReadableHeapSize(getHeapSizeMb());
    }
}
