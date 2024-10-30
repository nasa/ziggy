package gov.nasa.ziggy.pipeline.definition.database;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Group_;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link Group}
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class GroupCrud extends AbstractCrud<Group> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(GroupCrud.class);

    public GroupCrud() {
    }

    public GroupCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public Group retrieve(String name, String type) {
        if (StringUtils.isBlank(name) || StringUtils.isBlank(type)) {
            return Group.DEFAULT;
        }
        Group group = uniqueResult(createZiggyQuery(Group.class).column(Group_.name)
            .in(name)
            .column(Group_.type)
            .in(type));
        return group != null ? group : Group.DEFAULT;
    }

    public List<Group> retrieveAll() {
        return list(createZiggyQuery(Group.class).column(Group_.name).ascendingOrder());
    }

    public List<Group> retrieveAll(String type) {
        return list(createZiggyQuery(Group.class).column(Group_.type)
            .in(type)
            .column(Group_.name)
            .ascendingOrder());
    }

    @Override
    public Class<Group> componentClass() {
        return Group.class;
    }
}
