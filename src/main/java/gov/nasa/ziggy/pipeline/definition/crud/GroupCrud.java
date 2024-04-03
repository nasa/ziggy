package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Group_;
import gov.nasa.ziggy.pipeline.definition.Groupable;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link Group}
 *
 * @author Todd Klaus
 */
public class GroupCrud extends AbstractCrud<Group> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(GroupCrud.class);

    public GroupCrud() {
    }

    public GroupCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<Group> retrieveAll() {
        List<Group> groups = list(
            createZiggyQuery(Group.class).column(Group_.name).ascendingOrder());
        for (Group group : groups) {
            Hibernate.initialize(group.getParameterSetNames());
            Hibernate.initialize(group.getPipelineDefinitionNames());
            Hibernate.initialize(group.getPipelineModuleNames());
        }
        return groups;
    }

    public List<Group> retrieveAll(Class<? extends Groupable> clazz) {
        List<Group> groups = retrieveAll();
        for (Group group : groups) {
            setGroupMemberNames(group, clazz);
        }
        return groups;
    }

    public Group retrieveGroupByName(String name, Class<? extends Groupable> clazz) {
        if (StringUtils.isEmpty(name)) {
            return Group.DEFAULT;
        }
        Group group = uniqueResult(createZiggyQuery(Group.class).column(Group_.name).in(name));
        setGroupMemberNames(group, clazz);
        return group;
    }

    private void setGroupMemberNames(Group group, Class<? extends Groupable> clazz) {
        if (clazz.equals(PipelineDefinition.class)) {
            Hibernate.initialize(group.getPipelineDefinitionNames());
            group.setMemberNames(group.getPipelineDefinitionNames());
        }
        if (clazz.equals(ParameterSet.class)) {
            Hibernate.initialize(group.getParameterSetNames());
            group.setMemberNames(group.getParameterSetNames());
        }
    }

    @Override
    public Class<Group> componentClass() {
        return Group.class;
    }
}
