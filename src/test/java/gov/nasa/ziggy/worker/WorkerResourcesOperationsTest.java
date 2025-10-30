package gov.nasa.ziggy.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;

public class WorkerResourcesOperationsTest {

    private WorkerResourcesOperations workerResourcesOperations = Mockito
        .spy(WorkerResourcesOperations.class);
    private WorkerResources defaultResources = new WorkerResources(1, 1);
    private WorkerResources pipelineNodeResources = new WorkerResources(0, 0);
    private PipelineNode pipelineNode = Mockito.mock(PipelineNode.class);

    @Rule
    public ZiggyDatabaseRule ziggyDatabaseRule = new ZiggyDatabaseRule();

    @Test
    public void testCompositeWorkerResources() {
        Mockito.doReturn(defaultResources).when(workerResourcesOperations).defaultInstance();
        Mockito.doReturn(pipelineNodeResources)
            .when(workerResourcesOperations)
            .nodeResources(pipelineNode);
        WorkerResources compositeResources = workerResourcesOperations
            .compositeWorkerResources(pipelineNode);
        assertEquals(1, compositeResources.getMaxWorkerCount());
        assertEquals(1, compositeResources.getHeapSizeGigabytes(), 1e-3);
        pipelineNodeResources.setHeapSizeGigabytes(10);
        compositeResources = workerResourcesOperations.compositeWorkerResources(pipelineNode);
        assertEquals(1, compositeResources.getMaxWorkerCount());
        assertEquals(10, compositeResources.getHeapSizeGigabytes(), 1e-3);
        pipelineNodeResources.setMaxWorkerCount(5);
        compositeResources = workerResourcesOperations.compositeWorkerResources(pipelineNode);
        assertEquals(5, compositeResources.getMaxWorkerCount());
        assertEquals(10, compositeResources.getHeapSizeGigabytes(), 1e-3);
        pipelineNodeResources.setHeapSizeGigabytes(0);
        compositeResources = workerResourcesOperations.compositeWorkerResources(pipelineNode);
        assertEquals(5, compositeResources.getMaxWorkerCount());
        assertEquals(1, compositeResources.getHeapSizeGigabytes(), 1e-3);
    }

    @Test
    public void testUpdateWorkerResources() {
        assertNull(workerResourcesOperations.defaultInstance());
        workerResourcesOperations.updateDefaultInstance(defaultResources);
        WorkerResources databaseResources = workerResourcesOperations.defaultInstance();
        assertNotNull(databaseResources);
        assertEquals(defaultResources.getMaxWorkerCount(), databaseResources.getMaxWorkerCount());
        assertEquals(defaultResources.getHeapSizeGigabytes(),
            databaseResources.getHeapSizeGigabytes(), 1e-3);
        WorkerResources replacementResources = new WorkerResources(2, 2);
        workerResourcesOperations.updateDefaultInstance(replacementResources);
        WorkerResources newDefaultResources = workerResourcesOperations.defaultInstance();
        assertEquals(replacementResources.getMaxWorkerCount(),
            newDefaultResources.getMaxWorkerCount());
        assertEquals(replacementResources.getHeapSizeGigabytes(),
            newDefaultResources.getHeapSizeGigabytes(), 1e-3);
        assertEquals(databaseResources.getId(), newDefaultResources.getId());
    }
}
