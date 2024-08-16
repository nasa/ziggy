package gov.nasa.ziggy.module;

import java.nio.file.Path;
import java.util.Set;

import gov.nasa.ziggy.module.io.ProxyIgnore;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

public class PipelineInputsOutputsForTest implements PipelineInputs, PipelineOutputs {

    @ProxyIgnore
    private Path taskDirectory;

    @ProxyIgnore
    private PipelineTask pipelineTask;

    private int intValue = 7;
    private float floatValue = 12.5F;

    @Override
    public void copyDatastoreFilesToTaskDirectory(TaskConfiguration taskConfiguration,
        Path taskDirectory) {
    }

    @Override
    public SubtaskInformation subtaskInformation(PipelineDefinitionNode pipelineDefinitionNode) {
        return null;
    }

    @Override
    public void beforeAlgorithmExecution() {
    }

    @Override
    public void writeParameterSetsToTaskDirectory() {
    }

    @Override
    public void setPipelineTask(PipelineTask pipelineTask) {
    }

    @Override
    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    @Override
    public void setTaskDirectory(Path taskDirectory) {
        this.taskDirectory = taskDirectory;
    }

    @Override
    public Path getTaskDirectory() {
        return taskDirectory;
    }

    public void setIntValue(int intValue) {
        this.intValue = intValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public void setFloatValue(float floatValue) {
        this.floatValue = floatValue;
    }

    public float getFloatValue() {
        return floatValue;
    }

    @Override
    public Set<Path> copyTaskFilesToDatastore() {
        return null;
    }

    @Override
    public boolean subtaskProducedOutputs() {
        return false;
    }

    @Override
    public void afterAlgorithmExecution() {
    }
}
