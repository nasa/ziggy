package gov.nasa.ziggy.pipeline.definition;

/**
 * A database entity that has a {@link Group}.
 *
 * @author PT
 */
public interface HasGroup {

    Group group();

    default String groupName() {
        return group() != null ? group().getName() : "default";
    }

    void setGroup(Group group);
}
