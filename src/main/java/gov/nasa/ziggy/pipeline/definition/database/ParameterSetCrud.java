package gov.nasa.ziggy.pipeline.definition.database;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSet_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Provides CRUD methods for {@link ParameterSet}.
 *
 * @author Todd Klaus
 * @author PT
 */
public class ParameterSetCrud extends UniqueNameVersionPipelineComponentCrud<ParameterSet> {

    public ParameterSetCrud() {
    }

    public List<ParameterSet> retrieveAll() {
        List<ParameterSet> result = list(createZiggyQuery(ParameterSet.class));
        return visibleParameterSets(result);
    }

    public List<ParameterSet> visibleParameterSets(List<ParameterSet> allParameterSets) {
        return allParameterSets.stream().collect(Collectors.toList());
    }

    public ParameterSet retrieve(long id) {
        ZiggyQuery<ParameterSet, ParameterSet> query = createZiggyQuery(ParameterSet.class);
        query.column(ParameterSet_.id).in(id);
        ParameterSet result = uniqueResult(query);
        return result;
    }

    public Set<ParameterSet> retrieveParameterSets(PipelineTask pipelineTask) {
        Set<ParameterSet> parameterSets = new HashSet<>();
        parameterSets.addAll(
            new PipelineInstanceCrud().retrieveParameterSets(pipelineTask.getPipelineInstanceId()));
        parameterSets.addAll(new PipelineInstanceNodeCrud().retrieveParameterSets(pipelineTask));
        return parameterSets;
    }

    public ParameterSet merge(ParameterSet parameterSet) {
        parameterSet.setParameters(parameterSet.copyOfParameters());
        return super.merge(parameterSet);
    }

    @Override
    public String componentNameForExceptionMessages() {
        return "parameter set";
    }

    @Override
    public Class<ParameterSet> componentClass() {
        return ParameterSet.class;
    }
}
