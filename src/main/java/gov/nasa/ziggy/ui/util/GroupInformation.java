package gov.nasa.ziggy.ui.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.Groupable;
import gov.nasa.ziggy.ui.util.proxy.GroupCrudProxy;

public class GroupInformation<T extends Groupable> {

    private List<T> defaultGroup = new LinkedList<>();
    private Map<Group, List<T>> groups = new HashMap<>();
    private Map<String, T> objectsByName = new HashMap<>();
    private Map<T, Group> objectGroups = new HashMap<>();

    private final GroupCrudProxy groupCrudProxy = new GroupCrudProxy();

    private final Class<T> clazz;

    public GroupInformation(Class<T> clazz, List<T> allObjects) {
        this.clazz = clazz;
        initialize(allObjects);
    }

    private void initialize(List<T> allObjects) {
        List<Group> allGroups = groupCrudProxy.retrieveAll(clazz);

        defaultGroup = new LinkedList<>();
        groups = new HashMap<>();
        objectsByName = new HashMap<>();

        for (T object : allObjects) {
            objectsByName.put(object.getName(), object);
        }

        List<T> groupList = new ArrayList<>();
        for (Group group : allGroups) {

            // Does this group contain any of the objects we're interested in today?
            Sets.SetView<String> objectsThisGroup = Sets.intersection(group.getMemberNames(),
                objectsByName.keySet());
            if (!objectsThisGroup.isEmpty()) {
                List<T> objects = new ArrayList<>();
                for (String objectName : objectsThisGroup) {
                    objects.add(objectsByName.get(objectName));
                }
                groups.put(group, objects);
                groupList.addAll(objects);
            }
        }

        // Now populate the default group.
        List<T> objectsWithNoGroup = new ArrayList<>(objectsByName.values());
        objectsWithNoGroup.removeAll(groupList);
        defaultGroup.addAll(objectsWithNoGroup);

        // Populate the inverse map.
        for (Map.Entry<Group, List<T>> entry : groups.entrySet()) {
            for (T object : entry.getValue()) {
                objectGroups.put(object, entry.getKey());
            }
        }
        for (T object : objectsWithNoGroup) {
            objectGroups.put(object, Group.DEFAULT);
        }
    }

    public Map<Group, List<T>> getGroups() {
        return groups;
    }

    public List<T> getDefaultGroup() {
        return defaultGroup;
    }

    public Map<T, Group> getObjectGroups() {
        return objectGroups;
    }

    public Map<String, T> getObjectsByName() {
        return objectsByName;
    }
}
