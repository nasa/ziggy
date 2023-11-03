package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.util.HumanReadableHeapSize;

/**
 * Notifies the {@link ZiggyGuiConsole} of the default settings for worker count and Java heap size
 * that are stored in the {@link PipelineSupervisor}. This information is also used internally to
 * determine the correct worker count and heap size for a given {@link PipelineDefinitionNode},
 * based on whether the node has any non-default values set.
 * <p>
 * The class contains a singleton instance of {@link WorkerResources} that holds the default values
 * for the pipeline. If a given pipeline definition node has no values set for one or both
 * parameters, the default values will be returned by the class getter methods. There are also
 * boolean methods that indicate whether the default or per-node values are being returned.
 *
 * @author PT
 */
public class WorkerResources extends PipelineMessage {

    private static final long serialVersionUID = 20230714L;

    /**
     * Singleton instance that specifies the default values for the resources.
     */
    private static WorkerResources defaultResources;

    // Values are boxed so they can be null.
    private Integer maxWorkerCount;
    private Integer heapSizeMb;

    public WorkerResources(Integer maxWorkerCount, Integer heapSizeMb) {
        this.maxWorkerCount = maxWorkerCount;
        this.heapSizeMb = heapSizeMb;
    }

    public int getMaxWorkerCount() {
        return !maxWorkerCountIsDefault() ? maxWorkerCount : defaultResources.getMaxWorkerCount();
    }

    public int getHeapSizeMb() {
        return !heapSizeIsDefault() ? heapSizeMb : defaultResources.getHeapSizeMb();
    }

    public HumanReadableHeapSize humanReadableHeapSize() {
        return new HumanReadableHeapSize(heapSizeMb);
    }

    public boolean heapSizeIsDefault() {
        return heapSizeMb == null;
    }

    public boolean maxWorkerCountIsDefault() {
        return maxWorkerCount == null;
    }

    public static void setDefaultResources(WorkerResources resources) {
        defaultResources = resources;
    }

    public static WorkerResources getDefaultResources() {
        return defaultResources;
    }
}
