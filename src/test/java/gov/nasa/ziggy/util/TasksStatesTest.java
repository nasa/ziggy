package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.util.TasksStates.Summary;

public class TasksStatesTest {

    @Test
    public void testUpdate() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(new HashMap<>(), tasksStates.getModuleStates());

        tasksStates.update(pipelineTasks(), taskAttributes());
        testModuleStates(tasksStates);
    }

    @Test
    public void testGetModuleStates() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(new HashMap<>(), tasksStates.getModuleStates());

        tasksStates = new TasksStates(pipelineTasks(), taskAttributes());
        testModuleStates(tasksStates);
    }

    @Test
    public void testGetModuleNames() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(new ArrayList<>(), tasksStates.getModuleNames());

        tasksStates = new TasksStates(pipelineTasks(), taskAttributes());
        assertEquals(5, tasksStates.getModuleNames().size());
        assertEquals("module1", tasksStates.getModuleNames().get(0));
        assertEquals("module2", tasksStates.getModuleNames().get(1));
        assertEquals("module3", tasksStates.getModuleNames().get(2));
        assertEquals("module4", tasksStates.getModuleNames().get(3));
        assertEquals("module5", tasksStates.getModuleNames().get(4));
    }

    @Test
    public void testGetTotalSubmittedCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalSubmittedCount());
    }

    @Test
    public void testGetTotalProcessingCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalProcessingCount());
    }

    @Test
    public void testGetTotalErrorCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalErrorCount());
    }

    @Test
    public void testGetTotalCompletedCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalCompletedCount());
    }

    @Test
    public void testGetTotalSubTaskTotalCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalSubTaskTotalCount());
    }

    @Test
    public void testGetTotalSubTaskCompleteCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalSubTaskCompleteCount());
    }

    @Test
    public void testGetTotalSubTaskFailedCount() {
        TasksStates tasksStates = new TasksStates();
        assertEquals(0, tasksStates.getTotalSubTaskFailedCount());
    }

    private List<PipelineTask> pipelineTasks() {
        return List.of(pipelineTask("module1", 1L, 10, State.INITIALIZED),
            pipelineTask("module2", 2L, 20, State.SUBMITTED),
            pipelineTask("module3", 3L, 30, State.PROCESSING),
            pipelineTask("module4", 4L, 40, State.ERROR),
            pipelineTask("module5", 5L, 50, State.COMPLETED));
    }

    private PipelineTask pipelineTask(String moduleName, Long id, int attributeSeed, State state) {
        PipelineTask pipelineTask = new PipelineTask();
        PipelineInstanceNode pipelineInstanceNode = new PipelineInstanceNode();
        pipelineInstanceNode.setPipelineModuleDefinition(new PipelineModuleDefinition(moduleName));
        pipelineTask.setPipelineInstanceNode(pipelineInstanceNode);
        pipelineTask.setId(id);
        pipelineTask.setTotalSubtaskCount(attributeSeed);
        pipelineTask.setCompletedSubtaskCount(attributeSeed - (int) (0.1 * attributeSeed));
        pipelineTask.setFailedSubtaskCount((int) (0.1 * attributeSeed));
        pipelineTask.setState(state);
        return pipelineTask;
    }

    private Map<Long, ProcessingSummary> taskAttributes() {
        List<PipelineTask> pipelineTasks = pipelineTasks();
        return Map.of(pipelineTasks.get(0).getId(), new ProcessingSummary(pipelineTasks.get(0)),
            pipelineTasks.get(1).getId(), new ProcessingSummary(pipelineTasks.get(1)),
            pipelineTasks.get(2).getId(), new ProcessingSummary(pipelineTasks.get(2)),
            pipelineTasks.get(3).getId(), new ProcessingSummary(pipelineTasks.get(3)),
            pipelineTasks.get(4).getId(), new ProcessingSummary(pipelineTasks.get(4)));
    }

    private void testModuleStates(TasksStates tasksStates) {
        Map<String, Summary> moduleStates = tasksStates.getModuleStates();
        assertEquals(5, moduleStates.size());

        Summary summary = moduleStates.get("module1");
        assertEquals(0, summary.getCompletedCount());
        assertEquals(0, summary.getErrorCount());
        assertEquals(0, summary.getProcessingCount());
        assertEquals(0, summary.getSubmittedCount());
        assertEquals(9, summary.getSubTaskCompleteCount());
        assertEquals(1, summary.getSubTaskFailedCount());
        assertEquals(10, summary.getSubTaskTotalCount());

        summary = moduleStates.get("module2");
        assertEquals(0, summary.getCompletedCount());
        assertEquals(0, summary.getErrorCount());
        assertEquals(0, summary.getProcessingCount());
        assertEquals(1, summary.getSubmittedCount());
        assertEquals(18, summary.getSubTaskCompleteCount());
        assertEquals(2, summary.getSubTaskFailedCount());
        assertEquals(20, summary.getSubTaskTotalCount());

        summary = moduleStates.get("module3");
        assertEquals(0, summary.getCompletedCount());
        assertEquals(0, summary.getErrorCount());
        assertEquals(1, summary.getProcessingCount());
        assertEquals(0, summary.getSubmittedCount());
        assertEquals(27, summary.getSubTaskCompleteCount());
        assertEquals(3, summary.getSubTaskFailedCount());
        assertEquals(30, summary.getSubTaskTotalCount());

        summary = moduleStates.get("module4");
        assertEquals(0, summary.getCompletedCount());
        assertEquals(1, summary.getErrorCount());
        assertEquals(0, summary.getProcessingCount());
        assertEquals(0, summary.getSubmittedCount());
        assertEquals(36, summary.getSubTaskCompleteCount());
        assertEquals(4, summary.getSubTaskFailedCount());
        assertEquals(40, summary.getSubTaskTotalCount());

        summary = moduleStates.get("module5");
        assertEquals(1, summary.getCompletedCount());
        assertEquals(0, summary.getErrorCount());
        assertEquals(0, summary.getProcessingCount());
        assertEquals(0, summary.getSubmittedCount());
        assertEquals(45, summary.getSubTaskCompleteCount());
        assertEquals(5, summary.getSubTaskFailedCount());
        assertEquals(50, summary.getSubTaskTotalCount());
    }
}
