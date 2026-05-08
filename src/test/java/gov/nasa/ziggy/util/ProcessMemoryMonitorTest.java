package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
import gov.nasa.ziggy.pipeline.step.hdf5.Hdf5AlgorithmInterface;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.util.ProcessMemoryMonitor.MaxMemorySample;
import gov.nasa.ziggy.util.ProcessMemoryMonitor.MemorySample;
import gov.nasa.ziggy.util.ProcessMemoryMonitor.MemorySampleSeries;
import gov.nasa.ziggy.util.os.ProcInfo;

/**
 * Unit test class for {@link ProcessMemoryMonitor}.
 * <p>
 * Note that the number of memory samples is not entirely deterministic. It depends on how long each
 * sleep command actually runs and the exact times at which the ProcessMemoryMonitor obtains a
 * sample.
 *
 * @author PT
 */
public class ProcessMemoryMonitorTest {

    private static final boolean MEMORY_MONITOR_ENABLED = true;
    private static final double MEMORY_MONITOR_INTERVAL_SECONDS = 0.02;

    @Rule
    public ZiggyDirectoryRule ziggyDirectoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule pipelineResultsDirRule = new ZiggyPropertyRule(
        PropertyName.RESULTS_DIR, ziggyDirectoryRule);

    private ProcInfo procInfo1000 = Mockito.mock(ProcInfo.class);
    private ProcInfo procInfo1001 = Mockito.mock(ProcInfo.class);
    private ProcessMemoryMonitor processMemoryMonitor;

    @Before
    public void setUp() {
        createTaskConfiguration("1-1-task", MEMORY_MONITOR_ENABLED);

        processMemoryMonitor = Mockito.spy(ProcessMemoryMonitor.newInstance("1-1-task", 100));
        Mockito.doReturn(procInfo1000).when(processMemoryMonitor).procInfo(1000L);
        Mockito.doReturn(procInfo1001).when(processMemoryMonitor).procInfo(1001L);
        Mockito.when(procInfo1000.getMemoryBytes()).thenReturn(100L);
        Mockito.when(procInfo1001.getMemoryBytes()).thenReturn(200L);
    }

    private void createTaskConfiguration(String taskDir, boolean memoryMonitorEnabled) {
        Path taskDirPath = DirectoryProperties.taskDataDir().resolve(taskDir);
        if (!Files.exists(taskDirPath)) {
            try {
                Files.createDirectories(taskDirPath);
            } catch (IOException ignore) {
            }
        }
        TaskConfiguration taskConfiguration = new TaskConfiguration(taskDirPath.toFile());
        taskConfiguration.setInputsClassName("ProcessMemoryPipelineInputs.class");
        taskConfiguration.setOutputsClassName("ProcessMemoryPipelineInputs.class");
        taskConfiguration.setExecutableName("foo");
        taskConfiguration.setMemoryMonitorIntervalSeconds(MEMORY_MONITOR_INTERVAL_SECONDS);
        taskConfiguration.setMemoryMonitorEnabled(memoryMonitorEnabled);
        System.out.println("serializing " + taskConfiguration.getTaskDir());
        taskConfiguration.serialize();
    }

    @Test
    public void testStartStop() throws InterruptedException {
        processMemoryMonitor.updateProcessId(1000L);
        Thread.sleep(105L);
        processMemoryMonitor.stopMonitoring();
        List<MemorySample> memorySamples = processMemoryMonitor.getMemorySampleSeries()
            .getMemorySamples();
        assertTrue(memorySamples.size() == 5 || memorySamples.size() == 6);
        long timestamp = memorySamples.get(0).getTimestamp();
        assertEquals(100L, memorySamples.get(0).getMemoryUsageBytes());
        for (int i = 1; i < memorySamples.size(); i++) {
            assertEquals(100L, memorySamples.get(i).getMemoryUsageBytes());
            assertTrue(memorySamples.get(i).getTimestamp() > timestamp);
            timestamp = memorySamples.get(i).getTimestamp();
        }
    }

    @Test
    public void testUpdateProcessId() throws InterruptedException {
        processMemoryMonitor.updateProcessId(1000L);
        Thread.sleep(62L);
        processMemoryMonitor.updateProcessId(1001L);
        Thread.sleep(42L);
        processMemoryMonitor.stopMonitoring();
        List<MemorySample> memorySamples = processMemoryMonitor.getMemorySampleSeries()
            .getMemorySamples();
        assertTrue(memorySamples.size() >= 5 && memorySamples.size() <= 7);
        assertEquals(100L, memorySamples.get(0).getMemoryUsageBytes());
        assertEquals(100L, memorySamples.get(1).getMemoryUsageBytes());
        assertEquals(100L, memorySamples.get(2).getMemoryUsageBytes());
        assertTrue(memorySamples.get(3).getMemoryUsageBytes() == 100
            || memorySamples.get(3).getMemoryUsageBytes() == 200);
        for (int sampleCounter = 4; sampleCounter < memorySamples.size(); sampleCounter++) {
            assertEquals(200L, memorySamples.get(sampleCounter).getMemoryUsageBytes());
        }
    }

    @Test
    public void testHandleMissingSamples() throws InterruptedException {
        Mockito.when(procInfo1000.getMemoryBytes())
            .thenReturn(100L)
            .thenReturn(-1L)
            .thenReturn(200L)
            .thenReturn(-1L)
            .thenReturn(300L);
        processMemoryMonitor.updateProcessId(1000L);
        Thread.sleep(105L);
        processMemoryMonitor.stopMonitoring();
        List<MemorySample> memorySamples = processMemoryMonitor.getMemorySampleSeries()
            .getMemorySamples();
        assertTrue(memorySamples.size() == 3 || memorySamples.size() == 4);
        assertEquals(100L, memorySamples.get(0).getMemoryUsageBytes());
        assertEquals(200L, memorySamples.get(1).getMemoryUsageBytes());
        assertEquals(300L, memorySamples.get(2).getMemoryUsageBytes());
    }

    @Test
    public void testWriteMemorySamples() throws InterruptedException {
        Mockito.when(procInfo1000.getMemoryBytes())
            .thenReturn(100L)
            .thenReturn(200L)
            .thenReturn(300L)
            .thenReturn(200L)
            .thenReturn(100L);
        processMemoryMonitor.updateProcessId(1000L);
        Thread.sleep(105L);
        processMemoryMonitor.stopMonitoring();
        processMemoryMonitor.writeMemorySamples();
        assertTrue(Files.isRegularFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve("1-1-task")
            .resolve("st-100-mem-usage.h5")));
        assertTrue(Files.isRegularFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve("1-1-task")
            .resolve("st-100-max-mem-usage.h5")));
        Hdf5AlgorithmInterface hdf5AlgorithmInterface = new Hdf5AlgorithmInterface();
        MemorySampleSeries memorySamples = new MemorySampleSeries();
        hdf5AlgorithmInterface.readFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve("1-1-task")
            .resolve("st-100-mem-usage.h5")
            .toFile(), memorySamples, false);
        List<MemorySample> originalMemorySamples = processMemoryMonitor.getMemorySampleSeries()
            .getMemorySamples();
        List<MemorySample> serializedMemorySamples = memorySamples.getMemorySamples();
        assertTrue(serializedMemorySamples.size() == 5 || serializedMemorySamples.size() == 6);
        for (int i = 0; i < serializedMemorySamples.size(); i++) {
            assertEquals(originalMemorySamples.get(i), serializedMemorySamples.get(i));
        }
        MaxMemorySample maxMemorySample = new MaxMemorySample();
        hdf5AlgorithmInterface.readFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve("1-1-task")
            .resolve("st-100-max-mem-usage.h5")
            .toFile(), maxMemorySample, false);
        assertEquals(1L, maxMemorySample.getPipelineTaskId());
        assertEquals(100, maxMemorySample.getSubtaskIndex());
        assertEquals(originalMemorySamples.get(2), maxMemorySample.getMaxMemorySample());
    }

    @Test
    public void testMaxMemorySamplesDescendingOrder() throws InterruptedException {
        ProcInfo procInfo1002 = Mockito.mock(ProcInfo.class);
        ProcInfo procInfo1003 = Mockito.mock(ProcInfo.class);
        createTaskConfiguration("1-2-task", MEMORY_MONITOR_ENABLED);
        ProcessMemoryMonitor processMemoryMonitor2 = Mockito
            .spy(ProcessMemoryMonitor.newInstance("1-2-task", 120));
        Mockito.doReturn(procInfo1001).when(processMemoryMonitor2).procInfo(1001L);
        ProcessMemoryMonitor processMemoryMonitor3 = Mockito
            .spy(ProcessMemoryMonitor.newInstance("1-1-task", 120));
        Mockito.doReturn(procInfo1002).when(processMemoryMonitor3).procInfo(1002L);
        Mockito.doReturn(procInfo1001).when(processMemoryMonitor2).procInfo(1001L);
        ProcessMemoryMonitor processMemoryMonitor4 = Mockito
            .spy(ProcessMemoryMonitor.newInstance("1-2-task", 100));
        Mockito.doReturn(procInfo1003).when(processMemoryMonitor4).procInfo(1003L);
        Mockito.when(procInfo1000.getMemoryBytes())
            .thenReturn(100L)
            .thenReturn(200L)
            .thenReturn(300L)
            .thenReturn(200L)
            .thenReturn(100L);
        Mockito.when(procInfo1001.getMemoryBytes())
            .thenReturn(100L)
            .thenReturn(200L)
            .thenReturn(300L)
            .thenReturn(400L)
            .thenReturn(500L);
        Mockito.when(procInfo1002.getMemoryBytes()).thenReturn(100L);
        Mockito.when(procInfo1003.getMemoryBytes()).thenReturn(200L);
        processMemoryMonitor.updateProcessId(1000L);
        processMemoryMonitor2.updateProcessId(1001L);
        processMemoryMonitor3.updateProcessId(1002L);
        processMemoryMonitor4.updateProcessId(1003L);
        Thread.sleep(100L);
        processMemoryMonitor.stopMonitoring();
        processMemoryMonitor2.stopMonitoring();
        processMemoryMonitor3.stopMonitoring();
        processMemoryMonitor4.stopMonitoring();
        processMemoryMonitor.writeMemorySamples();
        processMemoryMonitor2.writeMemorySamples();
        processMemoryMonitor3.writeMemorySamples();
        processMemoryMonitor4.writeMemorySamples();
        PipelineTask pipelineTask1 = Mockito.spy(PipelineTask.class);
        Mockito.when(pipelineTask1.getId()).thenReturn(1L);
        Mockito.when(pipelineTask1.getPipelineInstanceId()).thenReturn(1L);
        Mockito.when(pipelineTask1.getPipelineStepName()).thenReturn("task");
        PipelineTask pipelineTask2 = Mockito.spy(PipelineTask.class);
        Mockito.when(pipelineTask2.getId()).thenReturn(2L);
        Mockito.when(pipelineTask2.getPipelineInstanceId()).thenReturn(1L);
        Mockito.when(pipelineTask2.getPipelineStepName()).thenReturn("task");
        List<MaxMemorySample> maxMemorySamples = ProcessMemoryMonitor
            .maxMemorySamplesDescendingOrder(List.of(pipelineTask1, pipelineTask2));
        assertEquals(4, maxMemorySamples.size());

        MaxMemorySample maxMemorySample = maxMemorySamples.get(0);
        assertEquals(2L, maxMemorySample.getPipelineTaskId());
        assertEquals(120, maxMemorySample.getSubtaskIndex());
        assertEquals(500L, maxMemorySample.getMaxMemorySample().getMemoryUsageBytes());

        maxMemorySample = maxMemorySamples.get(1);
        assertEquals(1L, maxMemorySample.getPipelineTaskId());
        assertEquals(100, maxMemorySample.getSubtaskIndex());
        assertEquals(300L, maxMemorySample.getMaxMemorySample().getMemoryUsageBytes());

        maxMemorySample = maxMemorySamples.get(2);
        assertEquals(2L, maxMemorySample.getPipelineTaskId());
        assertEquals(100, maxMemorySample.getSubtaskIndex());
        assertEquals(200L, maxMemorySample.getMaxMemorySample().getMemoryUsageBytes());

        maxMemorySample = maxMemorySamples.get(3);
        assertEquals(1L, maxMemorySample.getPipelineTaskId());
        assertEquals(120, maxMemorySample.getSubtaskIndex());
        assertEquals(100L, maxMemorySample.getMaxMemorySample().getMemoryUsageBytes());
    }

    @Test
    public void testDisabledProcessMemoryMonitor() throws InterruptedException {

        createTaskConfiguration("1-1-task", false);
        processMemoryMonitor = Mockito.spy(ProcessMemoryMonitor.newInstance("1-1-task", 100));
        processMemoryMonitor.updateProcessId(1000L);
        Thread.sleep(105L);
        processMemoryMonitor.stopMonitoring();
        processMemoryMonitor.writeMemorySamples();
        assertTrue(processMemoryMonitor.getMemorySampleSeries().getMemorySamples().isEmpty());
        assertFalse(Files.isRegularFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve("1-1-task")
            .resolve("st-100-mem-usage.h5")));
        assertFalse(Files.isRegularFile(ziggyDirectoryRule.directory()
            .resolve("run")
            .resolve("1-1-task")
            .resolve("st-100-max-mem-usage.h5")));
    }
}
