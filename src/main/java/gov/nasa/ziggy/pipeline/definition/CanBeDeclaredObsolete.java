package gov.nasa.ziggy.pipeline.definition;

/**
 * Interface that must be implemented for any pipeline infrastructure class that can be declared
 * obsolete (involves changing its group and its name).
 *
 * @author PT
 */
public interface CanBeDeclaredObsolete {

    Group getGroup();

    void setGroup(Group group);

    String getGroupName();

    void rename(String name);

    String getDatabaseName();

}
