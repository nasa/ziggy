package gov.nasa.ziggy.ui.util;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.NEW_SYMBOL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.CANCEL;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.OK;
import static gov.nasa.ziggy.ui.ZiggyGuiConstants.DELETE_SYMBOL;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.boldLabel;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButton;
import static gov.nasa.ziggy.ui.util.ZiggySwingUtils.createButtonPanel;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
import javax.swing.SwingWorker;
import javax.swing.event.ListSelectionEvent;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.pipeline.definition.database.GroupOperations;
import gov.nasa.ziggy.ui.ZiggyGuiConstants;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils.ButtonPanelContext;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
@SuppressWarnings("serial")
public class GroupsDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(GroupsDialog.class);

    private JCheckBox defaultCheckBox;
    private JTextField newGroupTextField;
    private JList<Group> groupsList;
    private GenericListModel<Group> groupsListModel;
    private boolean isCancelled;

    private final GroupOperations groupOperations = new GroupOperations();

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
        ArrayList<Group> groups = new ArrayList<>();
        groups.add(new Group(ZiggyGuiConstants.LOADING));
        groupsListModel.setList(groups);
        groupsList = new JList<>(groupsListModel);
        groupsList.addListSelectionListener(this::groupsListValueChanged);
        JScrollPane groupsListScrollPane = new JScrollPane(groupsList);
        groupsListScrollPane.setMinimumSize(new Dimension(300, 100));

        JLabel addRemoveGroup = boldLabel("Add/remove group");
        JPanel addRemoveButtons = createButtonPanel(ButtonPanelContext.TOOL_BAR,
            createButton(NEW_SYMBOL, this::addGroup), createButton(DELETE_SYMBOL, this::remove));

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
        String groupName = newGroupTextField.getText();
        if (StringUtils.isBlank(groupName)) {
            return;
        }

        List<Group> currentList = groupsListModel.getList();
        Group newGroup = new Group(groupName);
        if (currentList.contains(newGroup)) {
            MessageUtils.showError(this, "A group by that name already exists");
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                groupOperations().persist(newGroup);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    loadFromDatabase(groupName);
                } catch (Exception e) {
                    MessageUtils.showError(GroupsDialog.this, e);
                }
            }
        }.execute();
    }

    private void remove(ActionEvent evt) {
        Group group = getSelectedGroup();
        if (group == null) {
            return;
        }

        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                groupOperations().delete(group);
                return null;
            }

            @Override
            protected void done() {
                try {
                    get(); // check for exception
                    loadFromDatabase();
                } catch (Exception e) {
                    MessageUtils.showError(GroupsDialog.this, e);
                }
            }
        }.execute();
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
        loadFromDatabase(null);
    }

    private void loadFromDatabase(String selectedName) {
        new SwingWorker<List<Group>, Void>() {
            @Override
            protected List<Group> doInBackground() throws Exception {
                return groupOperations().groups();
            }

            @Override
            protected void done() {
                try {
                    groupsListModel.setList(get());

                    if (selectedName == null) {
                        return;
                    }

                    int index = 0;
                    for (Group group : groupsListModel.getList()) {
                        if (group.getName().equals(selectedName)) {
                            groupsList.setSelectedIndex(index);
                            // TODO Add break?
                        }
                        index++;
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Could not load groups", e);
                }
            }
        }.execute();
    }

    private GroupOperations groupOperations() {
        return groupOperations;
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
