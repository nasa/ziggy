package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.List;

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
        return list(createZiggyQuery(Group.class).column(Group_.name).ascendingOrder());
    }

    @Override
    public Class<Group> componentClass() {
        return Group.class;
    }
}
