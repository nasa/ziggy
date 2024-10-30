package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.pipeline.definition.Group;

/**
 * Unit tests for {@link GroupOperations}.
 *
 * @author Bill Wohler
 */
public class GroupOperationsTest {

    private static final String PARAMETER_SET = "ParameterSet";
    private static final String PIPELINE_DEFINITION = "PipelineDefinition";

    private GroupOperations groupOperations = new GroupOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Test
    public void testPersist() {
        Group group1 = createGroup1();
        assertEquals(group1, groupOperations.group("group1", PARAMETER_SET));
    }

    @Test
    public void testMerge() {
        Group group1 = createGroup1();
        assertEquals(group1, groupOperations.group("group1", PARAMETER_SET));

        group1.setName("newGroup");
        groupOperations.merge(group1);
        assertEquals("newGroup", groupOperations.group("newGroup", PARAMETER_SET).getName());
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
    public void testGroupsForType() {
        Group group1 = createGroup1();
        List<Group> groups = groupOperations.groups(PARAMETER_SET);
        assertEquals(1, groups.size());
        assertEquals(group1, groups.get(0));
        assertEquals(Set.of("parameterSet1"), groups.get(0).getItems());

        groups = groupOperations.groups(PIPELINE_DEFINITION);
        assertEquals(0, groups.size());
    }

    @Test
    public void testGroupForName() {
        assertTrue(groupOperations.group(null, PARAMETER_SET) == Group.DEFAULT);

        assertTrue(groupOperations.group("  ", PARAMETER_SET) == Group.DEFAULT);

        createGroup1();
        Group group = groupOperations.group("group1", PARAMETER_SET);
        assertEquals(Set.of("parameterSet1"), group.getItems());

        assertTrue(groupOperations.group("group1", PIPELINE_DEFINITION) == Group.DEFAULT);
        assertTrue(groupOperations.group("group2", PARAMETER_SET) == Group.DEFAULT);
    }

    @Test
    public void testDelete() {
        Group group1 = createGroup1();
        assertEquals(group1, groupOperations.group("group1", PARAMETER_SET));

        groupOperations.delete(group1);
        assertEquals(0, groupOperations.groups().size());
    }

    private Group createGroup1() {
        Group group1 = new Group("group1", PARAMETER_SET);
        group1.getItems().add("parameterSet1");
        groupOperations.persist(group1);
        return group1;
    }

    private Group createGroup2() {
        Group group2 = new Group("group2", PIPELINE_DEFINITION);
        group2.getItems().add("pipeline1");
        groupOperations.persist(group2);
        return group2;
    }
}
