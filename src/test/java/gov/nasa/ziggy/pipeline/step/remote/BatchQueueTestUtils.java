package gov.nasa.ziggy.pipeline.step.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;

public class BatchQueueTestUtils {

    public static List<BatchQueue> batchQueues() {
        BatchQueue lowQueue = mockedBatchQueue("low", "shortest", 4.0F, Integer.MAX_VALUE, false,
            true);
        BatchQueue normalQueue = mockedBatchQueue("normal", "normal", 8.0F, Integer.MAX_VALUE,
            false, true);
        BatchQueue longQueue = mockedBatchQueue("long", "longest", 120.0F, Integer.MAX_VALUE, false,
            true);
        BatchQueue develQueue = mockedBatchQueue("devel", "development", 2.0F, 1, false, false);
        BatchQueue debugQueue = mockedBatchQueue("debug", "debugging", 2.0F, 2, false, false);
        BatchQueue reservedQueue = mockedBatchQueue("reserved", "the most reserved",
            Float.MAX_VALUE, Integer.MAX_VALUE, true, false);
        return List.of(lowQueue, normalQueue, longQueue, develQueue, debugQueue, reservedQueue);
    }

    public static Map<String, BatchQueue> batchQueueByName() {
        List<BatchQueue> batchQueues = batchQueues();
        Map<String, BatchQueue> batchQueueByName = new HashMap<>();
        for (BatchQueue batchQueue : batchQueues) {
            batchQueueByName.put(batchQueue.getName(), batchQueue);
        }
        return batchQueueByName;
    }

    private static BatchQueue mockedBatchQueue(String name, String description, float maxHours,
        int maxNodes, boolean reserved, boolean autoSelectable) {
        BatchQueue batchQueue = Mockito.spy(BatchQueue.class);
        Mockito.doReturn(name).when(batchQueue).getName();
        Mockito.doReturn(description).when(batchQueue).getDescription();
        Mockito.doReturn(maxHours).when(batchQueue).getMaxWallTimeHours();
        Mockito.doReturn(maxNodes).when(batchQueue).getMaxNodes();
        Mockito.doReturn(reserved).when(batchQueue).isReserved();
        Mockito.doReturn(autoSelectable).when(batchQueue).isAutoSelectable();
        return batchQueue;
    }
}
