package gov.nasa.ziggy.uow;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;

public class SingleUnitOfWorkGenerator implements UnitOfWorkGenerator {

    public SingleUnitOfWorkGenerator() {
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    @Override
    public List<UnitOfWork> generateUnitsOfWork(PipelineInstanceNode pipelineInstanceNode) {
        List<UnitOfWork> tasks = new LinkedList<>();
        UnitOfWork prototypeTask = new UnitOfWork();
        tasks.add(prototypeTask);

        return tasks;
    }

    @Override
    public void setBriefState(UnitOfWork uow, PipelineInstanceNode pipelineInstanceNode) {
        uow.setBriefState("single");
    }
}
