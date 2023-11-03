package gov.nasa.ziggy.ui.util;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.ADD_SYMBOL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.OK;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.REMOVE_SYMBOL;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.GroupLayout;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingUtilities;
import javax.swing.event.ListSelectionEvent;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;
import gov.nasa.ziggy.ui.util.proxy.GroupCrudProxy;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class GroupsDialog extends javax.swing.JDialog {
    private JCheckBox defaultCheckBox;
    private JTextField newGroupTextField;
    private JList<Group> groupsList;
    private GenericListModel<Group> groupsListModel;
    private GroupCrudProxy groupCrud = new GroupCrudProxy();
    private boolean isCancelled;

    public GroupsDialog(Window owner) {
        super(owner, DEFAULT_MODALITY_TYPE);
        buildComponent();
        setLocationRelativeTo(owner);

        loadFromDatabase();
    }

    private void buildComponent() {
        setTitle("Select group");

        getContentPane().add(getDataPanel(), BorderLayout.CENTER);
        getContentPane().add(
            createButtonPanel(createButton(OK, this::ok), createButton(CANCEL, this::cancel)),
            BorderLayout.SOUTH);

        pack();
    }

    private JPanel getDataPanel() {
        defaultCheckBox = new JCheckBox();
        defaultCheckBox.setText("Default group");
        defaultCheckBox.addActionListener(this::useDefaultGroup);

        groupsListModel = new GenericListModel<>();
        groupsList = new JList<>(groupsListModel);
        groupsList.addListSelectionListener(this::groupsListValueChanged);
        JScrollPane groupsListScrollPane = new JScrollPane(groupsList);

        JLabel addRemoveGroup = boldLabel("Add/remove group");
        JPanel addRemoveButtons = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(ADD_SYMBOL, this::addGroup), createButton(REMOVE_SYMBOL, this::remove));

        newGroupTextField = new JTextField();
        newGroupTextField.addActionListener(this::addGroup);

        JPanel dataPanel = new JPanel();
        GroupLayout dataPanelLayout = new GroupLayout(dataPanel);
        dataPanelLayout.setAutoCreateContainerGaps(true);
        dataPanel.setLayout(dataPanelLayout);

        dataPanelLayout.setHorizontalGroup(dataPanelLayout.createParallelGroup()
            .addComponent(defaultCheckBox)
            .addComponent(groupsListScrollPane)
            .addComponent(addRemoveGroup)
            .addComponent(addRemoveButtons)
            .addComponent(newGroupTextField));

        dataPanelLayout.setVerticalGroup(dataPanelLayout.createSequentialGroup()
            .addComponent(defaultCheckBox)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(groupsListScrollPane)
            .addPreferredGap(ComponentPlacement.UNRELATED)
            .addComponent(addRemoveGroup)
            .addComponent(addRemoveButtons, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE)
            .addComponent(newGroupTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE,
                GroupLayout.PREFERRED_SIZE));

        return dataPanel;
    }

    private void useDefaultGroup(ActionEvent evt) {
        groupsList.clearSelection();
    }

    private void groupsListValueChanged(ListSelectionEvent evt) {
        defaultCheckBox.setSelected(groupsList.getSelectedIndex() == -1);
    }

    private void addGroup(ActionEvent evt) {
        try {
            String groupName = newGroupTextField.getText();
            if (groupName != null && groupName.length() > 0) {
                List<Group> currentList = groupsListModel.getList();
                Group newGroup = new Group(groupName);
                if (currentList.contains(newGroup)) {
                    MessageUtil.showError(this, "A group by that name already exists");
                } else {
                    groupCrud.save(newGroup);

                    loadFromDatabase(groupName);
                }
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void remove(ActionEvent evt) {
        try {
            Group group = getSelectedGroup();
            if (group != null) {
                groupCrud.delete(group);
                loadFromDatabase();
            }
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void ok(ActionEvent evt) {
        setVisible(false);
    }

    private void cancel(ActionEvent evt) {
        isCancelled = true;
        setVisible(false);
    }

    private Group getSelectedGroup() {
        int selectedIndex = groupsList.getSelectedIndex();
        if (selectedIndex >= 0) {
            return groupsListModel.get(selectedIndex);
        }
        return null;
    }

    private void loadFromDatabase() {
        List<Group> groups = groupCrud.retrieveAll();
        groupsListModel.setList(groups);
    }

    private void loadFromDatabase(String selectedName) {
        loadFromDatabase();

        List<Group> groups = groupsListModel.getList();
        int index = 0;

        for (Group group : groups) {
            if (group.getName().equals(selectedName)) {
                groupsList.setSelectedIndex(index);
            }
            index++;
        }
    }

    public static Group selectGroup(Component owner) {
        GroupsDialog editor = new GroupsDialog(SwingUtilities.getWindowAncestor(owner));
        editor.setVisible(true);

        if (editor.isCancelled) {
            return null;
        }
        if (editor.defaultCheckBox.isSelected()) {
            return Group.DEFAULT;
        }
        return editor.getSelectedGroup();
    }

    public static void main(String[] args) {
        ZiggySwingUtils.displayTestDialog(new GroupsDialog((JFrame) null));
    }
}
