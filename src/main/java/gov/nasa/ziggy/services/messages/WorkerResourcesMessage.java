package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.worker.WorkerResources;

/**
 * Sends information about worker resources in response to a {@link WorkerResourcesRequest}.
 * <p>
 * Each instance of {@link WorkerResourcesMessage} contains two instances of
 * {@link WorkerResources}: an instance for default values, and an instance for non-default values.
 * When the {@link PipelineSupervisor} publishes this message, only the default resources are
 * populated. All other publishers will populate only the non-default resources. Subscribers will
 * typically want to use one or the other, and can determine whether a given message is "for them"
 * by checking which {@link WorkerResources} instance is populated.
 * <p>
 * Note that the {@link WorkerResourcesMessage} cannot transport a non-default
 * {@link WorkerResources} instance if any of its resource values are null.
 */
public class WorkerResourcesMessage extends PipelineMessage {

    private static final long serialVersionUID = 20231204L;

    private final WorkerResources defaultResources;
    private final WorkerResources resources;

    public WorkerResourcesMessage(WorkerResources defaultResources, WorkerResources resources) {
        this.defaultResources = defaultResources;
        this.resources = resources;
        validate();
    }

    public void validate() {
        if (defaultResources != null && resources != null) {
            throw new IllegalArgumentException(
                "default resources and resources cannot both be non-null");
        }
        if (defaultResources == null && resources == null) {
            throw new IllegalArgumentException(
                "default resources and resources cannot both be null");
        }
        if (defaultResources != null && (defaultResources.getHeapSizeMb() == null
            || defaultResources.getHeapSizeMb() == 0 || defaultResources.getMaxWorkerCount() == null
            || defaultResources.getMaxWorkerCount() == 0)) {
            throw new IllegalArgumentException(
                "Default resources must not contain any zero values or null values");
        }
        if (resources != null
            && (resources.getHeapSizeMb() == null || resources.getMaxWorkerCount() == null)) {
            throw new IllegalArgumentException("Resources must not include null values");
        }
    }

    public WorkerResources getDefaultResources() {
        return defaultResources;
    }

    public WorkerResources getResources() {
        return resources;
    }
}
