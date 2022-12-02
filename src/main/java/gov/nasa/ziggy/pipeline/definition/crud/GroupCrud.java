package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.List;

import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link Group}
 *
 * @author Todd Klaus
 */
public class GroupCrud extends AbstractCrud {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(GroupCrud.class);

    public GroupCrud() {
    }

    public GroupCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<Group> retrieveAll() {
        Query q = createQuery("from Group order by name asc");
        return list(q);
    }

}
