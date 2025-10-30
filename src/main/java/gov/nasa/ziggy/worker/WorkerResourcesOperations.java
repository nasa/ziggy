package gov.nasa.ziggy.worker;

import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.database.PipelineNodeCrud;
import gov.nasa.ziggy.services.database.DatabaseOperations;

public class WorkerResourcesOperations extends DatabaseOperations {

    private WorkerResourcesCrud workerResourcesCrud = new WorkerResourcesCrud();
    private PipelineNodeCrud pipelineNodeCrud = new PipelineNodeCrud();

    /** Returns the default {@link WorkerResources} instance. */
    public WorkerResources defaultInstance() {
        return performTransaction(() -> workerResourcesCrud().retrieveDefaultResources());
    }

    public void updateDefaultInstance(WorkerResources newDefaultValuesInstance) {
        WorkerResources defaultInstance = defaultInstance();
        if (defaultInstance == null) {
            newDefaultValuesInstance.setDefaultInstance(true);
            performTransaction(() -> workerResourcesCrud().persist(newDefaultValuesInstance));
            return;
        }
        defaultInstance.setHeapSizeGigabytes(newDefaultValuesInstance.getHeapSizeGigabytes());
        defaultInstance.setMaxWorkerCount(newDefaultValuesInstance.getMaxWorkerCount());
        performTransaction(() -> workerResourcesCrud().merge(defaultInstance));
    }

    public WorkerResources merge(WorkerResources workerResources) {
        return performTransaction(() -> workerResourcesCrud().merge(workerResources));
    }

    /**
     * Returns an instance of {@link WorkerResources} that combines the default values with the
     * values for a given {@link PipelineNode}. Any nonzero values from the pipeline node worker
     * resources instance are used; zero values in the pipeline node worker resources are replaced
     * with values from the default instance.
     */
    public WorkerResources compositeWorkerResources(PipelineNode pipelineNode) {
        WorkerResources nodeResources = nodeResources(pipelineNode);
        WorkerResources defaultResources = defaultInstance();
        int compositeWorkerCount = nodeResources.getMaxWorkerCount() > 0
            ? nodeResources.getMaxWorkerCount()
            : defaultResources.getMaxWorkerCount();
        float compositeHeapSizeGigabytes = nodeResources.getHeapSizeGigabytes() > 0
            ? nodeResources.getHeapSizeGigabytes()
            : defaultResources.getHeapSizeGigabytes();
        return new WorkerResources(compositeWorkerCount, compositeHeapSizeGigabytes);
    }

    WorkerResources nodeResources(PipelineNode pipelineNode) {
        return performTransaction(() -> pipelineNodeCrud().retrieveExecutionResources(pipelineNode))
            .workerResources();
    }

    WorkerResourcesCrud workerResourcesCrud() {
        return workerResourcesCrud;
    }

    PipelineNodeCrud pipelineNodeCrud() {
        return pipelineNodeCrud;
    }
}
