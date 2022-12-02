package gov.nasa.ziggy.ui.config.group;

import java.util.ArrayList;
import java.util.List;

import javax.swing.ComboBoxModel;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.ui.models.AbstractDatabaseListModel;
import gov.nasa.ziggy.ui.proxy.GroupCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class GroupListModel extends AbstractDatabaseListModel<Group>
    implements ComboBoxModel<Group> {
    private List<Group> groups = new ArrayList<>();
    private Group selectedGroup = null;
    GroupCrudProxy groupCrud = new GroupCrudProxy();

    public GroupListModel() {
    }

    @Override
    public void loadFromDatabase() {
        if (groups != null) {
            groupCrud.evictAll(groups);
        }

        groups = groupCrud.retrieveAll();

        if (groups.size() > 0) {
            selectedGroup = groups.get(0);
        }

        fireContentsChanged(this, 0, groups.size() - 1);
    }

    @Override
    public Group getElementAt(int index) {
        validityCheck();
        return groups.get(index);
    }

    @Override
    public int getSize() {
        validityCheck();
        return groups.size();
    }

    @Override
    public Object getSelectedItem() {
        validityCheck();
        return selectedGroup;
    }

    @Override
    public void setSelectedItem(Object anItem) {
        validityCheck();
        selectedGroup = (Group) anItem;
    }
}
