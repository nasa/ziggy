package gov.nasa.ziggy.pipeline.definition.database;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Groupable;
import gov.nasa.ziggy.services.database.DatabaseOperations;

/**
 * Operations class for {@link Group} objects.
 *
 * @author Bill Wohler
 */
public class GroupOperations extends DatabaseOperations {
    private GroupCrud groupCrud = new GroupCrud();

    public void persist(Group group) {
        performTransaction(() -> groupCrud.persist(group));
    }

    public Group merge(Group group) {
        return performTransaction(() -> groupCrud.merge(group));
    }

    public Set<Group> merge(Set<Group> groups) {
        return performTransaction(() -> {
            Set<Group> mergedGroups = new HashSet<>();
            for (Group group : groups) {
                if (group != Group.DEFAULT) {
                    mergedGroups.add(groupCrud.merge(group));
                }
            }
            return mergedGroups;
        });
    }

    public List<Group> groups() {
        return performTransaction(() -> groupCrud.retrieveAll());
    }

    public List<Group> groupsForClass(Class<? extends Groupable> clazz) {
        return performTransaction(() -> groupCrud.retrieveAll(clazz));
    }

    public Group groupForName(String name, Class<? extends Groupable> clazz) {
        return performTransaction(() -> groupCrud.retrieveGroupByName(name, clazz));
    }

    public void delete(Group group) {
        performTransaction(() -> groupCrud.remove(group));
    }
}
