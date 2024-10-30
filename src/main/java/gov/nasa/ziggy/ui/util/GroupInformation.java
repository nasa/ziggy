package gov.nasa.ziggy.ui.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.SwingWorker;

import com.google.common.collect.Sets;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.database.GroupOperations;

/**
 * A container for a collection of items that can be organized by group.
 *
 * @author PT
 * @author Bill Wohler
 */
public class GroupInformation<T> {

    private Map<String, T> objectByName;
    private Map<Group, List<T>> objectsByGroup;
    private List<T> objectsInDefaultGroup;
    private Map<T, Group> groupByObject;

    private final GroupOperations groupOperations = new GroupOperations();

    /**
     * Creates a {@code GroupInformation} object. This makes a database call so it should be
     * performed in {@link SwingWorker#doInBackground()}.
     */
    public GroupInformation(String type, List<T> allObjects) {
        List<Group> allGroups = groupOperations().groups(type);

        objectByName = createObjectByName(allObjects);
        objectsByGroup = createObjectsByGroup(allGroups, objectByName);
        objectsInDefaultGroup = createObjectsInDefaultGroup(objectByName,
            collectGroupedObjects(objectsByGroup));
        groupByObject = createGroupByObject(objectsByGroup, objectsInDefaultGroup);
    }

    private Map<String, T> createObjectByName(List<T> allObjects) {
        Map<String, T> objectsByName = new HashMap<>();
        for (T object : allObjects) {
            objectsByName.put(object.toString(), object);
        }
        return objectsByName;
    }

    private Map<Group, List<T>> createObjectsByGroup(List<Group> allGroups,
        Map<String, T> objectByName) {

        Map<Group, List<T>> groups = new HashMap<>();
        for (Group group : allGroups) {

            // Does this group contain any of the objects we're interested in today?
            Sets.SetView<String> items = Sets.intersection(group.getItems(),
                objectByName.keySet());
            if (!items.isEmpty()) {
                List<T> objects = new ArrayList<>();
                for (String objectName : items) {
                    objects.add(objectByName.get(objectName));
                }
                groups.put(group, objects);
            }
        }
        return groups;
    }

    private List<T> collectGroupedObjects(Map<Group, List<T>> objectsByGroup) {
        List<T> allGroupedObjects = new ArrayList<>();
        for (List<T> objects : objectsByGroup.values()) {
            allGroupedObjects.addAll(objects);
        }
        return allGroupedObjects;
    }

    private List<T> createObjectsInDefaultGroup(Map<String, T> objectByName,
        List<T> allGroupedObjects) {
        List<T> objectsWithNoGroup = new ArrayList<>(objectByName.values());
        objectsWithNoGroup.removeAll(allGroupedObjects);
        return new LinkedList<>(objectsWithNoGroup);
    }

    private Map<T, Group> createGroupByObject(Map<Group, List<T>> objectsByGroup,
        List<T> objectsInDefaultGroup) {

        Map<T, Group> groupByObject = new HashMap<>();
        for (Map.Entry<Group, List<T>> entry : objectsByGroup.entrySet()) {
            for (T object : entry.getValue()) {
                groupByObject.put(object, entry.getKey());
            }
        }
        for (T object : objectsInDefaultGroup) {
            groupByObject.put(object, Group.DEFAULT);
        }

        return groupByObject;
    }

    public Map<String, T> getObjectByName() {
        return objectByName;
    }

    public Map<Group, List<T>> getObjectsByGroup() {
        return objectsByGroup;
    }

    public List<T> getObjectsInDefaultGroup() {
        return objectsInDefaultGroup;
    }

    public Map<T, Group> getGroupByObject() {
        return groupByObject;
    }

    private GroupOperations groupOperations() {
        return groupOperations;
    }
}
