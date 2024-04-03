package gov.nasa.ziggy.ui.util.proxy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Groupable;
import gov.nasa.ziggy.pipeline.definition.crud.GroupCrud;

public class GroupCrudProxy {
    public GroupCrudProxy() {
    }

    public void save(final Group group) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            crud.persist(group);
            return null;
        });
    }

    public void delete(final Group group) {
        CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            crud.remove(group);
            return null;
        });
    }

    public List<Group> retrieveAll() {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            return crud.retrieveAll();
        });
    }

    public List<Group> retrieveAll(Class<? extends Groupable> clazz) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            return crud.retrieveAll(clazz);
        });
    }

    public Group retrieveGroupByName(String name, Class<? extends Groupable> clazz) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            GroupCrud crud = new GroupCrud();
            return crud.retrieveGroupByName(name, clazz);
        });
    }

    public Set<Group> merge(Set<Group> groups) {
        return CrudProxyExecutor.executeSynchronousDatabaseTransaction(() -> {
            Set<Group> mergedGroups = new HashSet<>();
            GroupCrud crud = new GroupCrud();
            for (Group group : groups) {
                if (group != Group.DEFAULT) {
                    mergedGroups.add(crud.merge(group));
                }
            }
            return mergedGroups;
        });
    }

    public Group merge(Group group) {
        return CrudProxyExecutor
            .executeSynchronousDatabaseTransaction(() -> new GroupCrud().merge(group));
    }
}
