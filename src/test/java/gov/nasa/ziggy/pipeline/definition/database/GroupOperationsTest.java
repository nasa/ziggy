package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;

/**
 * Unit tests for {@link GroupOperations}.
 *
 * @author Bill Wohler
 */
public class GroupOperationsTest {

    // TODO Fix per ZIGGY-431
    // These tests, like the code they are testing, are non-sensical to me. If you ask for groups
    // for PipelineDefinition and there are none, you should get none.

    private GroupOperations groupOperations = new GroupOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testPersist() {
        Group group1 = createGroup1();
        assertEquals(group1, groupOperations.groupForName("group1", ParameterSet.class));
    }

    @Test
    public void testMerge() {
        Group group1 = createGroup1();
        assertEquals(group1, groupOperations.groupForName("group1", ParameterSet.class));

        group1.setName("newGroup");
        groupOperations.merge(group1);
        assertEquals("newGroup",
            groupOperations.groupForName("newGroup", ParameterSet.class).getName());
    }

    @Test
    public void testMergeGroups() {
        Group group1 = createGroup1();
        Group group2 = createGroup2();

        group1.setName("newGroup1");
        group2.setName("newGroup2");
        groupOperations.merge(Set.of(group1, group2));
        List<Group> groups = groupOperations.groups();
        assertEquals(2, groups.size());
        if (groups.get(0).getName().equals("newGroup1")) {
            assertEquals("newGroup1", groups.get(0).getName());
            assertEquals("newGroup2", groups.get(1).getName());
        } else {
            assertEquals("newGroup2", groups.get(0).getName());
            assertEquals("newGroup1", groups.get(1).getName());
        }
    }

    @Test
    public void testGroups() {
        Group group1 = createGroup1();
        Group group2 = createGroup2();

        List<Group> groups = groupOperations.groups();
        assertEquals(2, groups.size());
        assertTrue(groups.contains(group1));
        assertTrue(groups.contains(group2));
    }

    @Test
    public void testGroupsForClass() {
        Group group1 = createGroup1();
        List<Group> groups = groupOperations.groupsForClass(ParameterSet.class);
        assertEquals(1, groups.size());
        assertEquals(group1, groups.get(0));
        assertEquals(Set.of("parameterSet1"), groups.get(0).getMemberNames());

        groups = groupOperations.groupsForClass(PipelineDefinition.class);
        assertEquals(1, groups.size());
        assertEquals(group1, groups.get(0));
        assertEquals(Set.of(), groups.get(0).getMemberNames());
    }

    @Test
    public void testGroupForName() {
        assertTrue(groupOperations.groupForName(null, ParameterSet.class) == Group.DEFAULT);

        assertTrue(groupOperations.groupForName("  ", ParameterSet.class) == Group.DEFAULT);

        createGroup1();
        Group group = groupOperations.groupForName("group1", ParameterSet.class);
        assertEquals(Set.of("parameterSet1"), group.getMemberNames());

        group = groupOperations.groupForName("group1", PipelineDefinition.class);
        assertEquals(Set.of(), group.getMemberNames());
    }

    @Test
    public void testDelete() {
        Group group1 = createGroup1();
        assertEquals(group1, groupOperations.groupForName("group1", ParameterSet.class));

        groupOperations.delete(group1);
        assertEquals(0, groupOperations.groups().size());
    }

    private Group createGroup1() {
        Group group1 = new Group("group1");
        group1.getParameterSetNames().add("parameterSet1");
        groupOperations.persist(group1);
        return group1;
    }

    private Group createGroup2() {
        Group group2 = new Group("group2");
        group2.getPipelineDefinitionNames().add("pipeline1");
        groupOperations.persist(group2);
        return group2;
    }
}
