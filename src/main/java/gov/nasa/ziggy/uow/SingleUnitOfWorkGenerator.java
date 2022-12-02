package gov.nasa.ziggy.uow;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.parameters.Parameters;

public class SingleUnitOfWorkGenerator implements UnitOfWorkGenerator {

    public SingleUnitOfWorkGenerator() {
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    @Override
    public String briefState(UnitOfWork uow) {
        return "single";
    }

    @Override
    public List<Class<? extends Parameters>> requiredParameterClasses() {
        return Collections.emptyList();
    }

    @Override
    public List<UnitOfWork> generateTasks(Map<Class<? extends Parameters>, Parameters> parameters) {
        List<UnitOfWork> tasks = new LinkedList<>();
        UnitOfWork prototypeTask = new UnitOfWork();
        tasks.add(prototypeTask);

        return tasks;
    }

}
