package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.ParameterSet_;

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
        populateXmlFields(result);
        return visibleParameterSets(result);
    }

    public List<ParameterSet> visibleParameterSets(List<ParameterSet> allParameterSets) {
        return allParameterSets.stream().collect(Collectors.toList());
    }

    public ParameterSet retrieve(long id) {
        ZiggyQuery<ParameterSet, ParameterSet> query = createZiggyQuery(ParameterSet.class);
        query.column(ParameterSet_.id).in(id);
        ParameterSet result = uniqueResult(query);
        populateXmlFields(result);
        return result;
    }

    /**
     * Populates the XML fields for a {@link ParameterSet} instance. This ensures that the instance
     * has its XML and database fields consistent with each other upon retrieval from the database.
     */
    private void populateXmlFields(ParameterSet result) {
        if (result != null) {
            result.populateXmlFields();
        }
    }

    /**
     * Populates the XML fields for a {@link Collection} of {@link ParameterSet} instances. This
     * ensures that the instances have their XML and database fields consistent with each other upon
     * retrieval from the database.
     */
    @Override
    protected void populateXmlFields(Collection<ParameterSet> results) {
        for (ParameterSet result : results) {
            populateXmlFields(result);
        }
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
