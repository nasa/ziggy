package gov.nasa.ziggy.ui.config.group;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.Group;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.GroupCrudProxy;

/**
 * Prompt the user for a group selection. Also provides the ability to add/remove groups.
 *
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class GroupSelectorDialog extends javax.swing.JDialog {
    private static final Logger log = LoggerFactory.getLogger(GroupSelectorDialog.class);

    private boolean cancelled = false;
    private JPanel dataPanel;
    private JScrollPane groupListScrollPane;
    private JButton removeGroupButton;
    private JButton addGroupButton;
    private JPanel groupListEditPanel;
    private JList<Group> groupList;
    private JButton cancelButton;
    private JButton selectButton;
    private JPanel actionPanel;

    private GroupListModel groupListModel;

    public GroupSelectorDialog(JFrame frame) {
        super(frame, true);
        initGUI();
    }

    public static Group selectGroup() {
        GroupSelectorDialog dialog = ZiggyGuiConsole.newGroupSelectorDialog();
        dialog.setVisible(true); // blocks until user presses a button

        if (!dialog.cancelled) {
            return dialog.getSelectedGroup();
        }
        return null;
    }

    public Group getSelectedGroup() {
        int selectedIndex = groupList.getSelectedIndex();

        if (selectedIndex != -1) {
            return groupListModel.getElementAt(selectedIndex);
        }
        return null;
    }

    private void selectButtonActionPerformed(ActionEvent evt) {
        log.debug("selectButton.actionPerformed, event=" + evt);

        setVisible(false);
    }

    private void cancelButtonActionPerformed(ActionEvent evt) {
        log.debug("cancelButton.actionPerformed, event=" + evt);

        cancelled = true;

        setVisible(false);
    }

    private void addGroupButtonActionPerformed(ActionEvent evt) {
        log.debug("addGroupButton.actionPerformed, event=" + evt);

        try {
            String newGroupName = ZiggyGuiConsole.showInputDialog(
                "Enter the name for the new Group", "New Group", JOptionPane.PLAIN_MESSAGE);

            if (newGroupName == null || newGroupName.length() == 0) {
                MessageUtil.showError(this, "Please enter a group name");
                return;
            }

            Group group = new Group(newGroupName);

            GroupCrudProxy groupCrud = new GroupCrudProxy();
            groupCrud.save(group);

            groupListModel.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private void removeGroupButtonActionPerformed(ActionEvent evt) {
        log.debug("removeGroupButton.actionPerformed, event=" + evt);
    }

    private void initGUI() {
        try {
            {
                setTitle("Select Group");
            }
            getContentPane().add(getDataPanel(), BorderLayout.CENTER);
            getContentPane().add(getActionPanel(), BorderLayout.SOUTH);
            this.setSize(229, 377);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame();
            GroupSelectorDialog inst = new GroupSelectorDialog(frame);
            inst.setVisible(true);
        });
    }

    private JPanel getDataPanel() {
        if (dataPanel == null) {
            dataPanel = new JPanel();
            BorderLayout dataPanelLayout = new BorderLayout();
            dataPanel.setLayout(dataPanelLayout);
            dataPanel.add(getGroupListScrollPane(), BorderLayout.CENTER);
            dataPanel.add(getGroupListEditPanel(), BorderLayout.EAST);
        }
        return dataPanel;
    }

    private JPanel getActionPanel() {
        if (actionPanel == null) {
            actionPanel = new JPanel();
            FlowLayout actionPanelLayout = new FlowLayout();
            actionPanelLayout.setHgap(40);
            actionPanel.setLayout(actionPanelLayout);
            actionPanel.add(getSelectButton());
            actionPanel.add(getCancelButton());
        }
        return actionPanel;
    }

    private JButton getSelectButton() {
        if (selectButton == null) {
            selectButton = new JButton();
            selectButton.setText("Select");
            selectButton.addActionListener(this::selectButtonActionPerformed);
        }
        return selectButton;
    }

    private JButton getCancelButton() {
        if (cancelButton == null) {
            cancelButton = new JButton();
            cancelButton.setText("Cancel");
            cancelButton.addActionListener(this::cancelButtonActionPerformed);
        }
        return cancelButton;
    }

    private JScrollPane getGroupListScrollPane() {
        if (groupListScrollPane == null) {
            groupListScrollPane = new JScrollPane();
            groupListScrollPane.setViewportView(getGroupList());
        }
        return groupListScrollPane;
    }

    private JList<Group> getGroupList() {
        if (groupList == null) {
            groupListModel = new GroupListModel();
            groupList = new JList<>();
            groupList.setModel(groupListModel);
        }
        return groupList;
    }

    private JPanel getGroupListEditPanel() {
        if (groupListEditPanel == null) {
            groupListEditPanel = new JPanel();
            GridBagLayout groupListEditPanelLayout = new GridBagLayout();
            groupListEditPanelLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1, 0.1, 0.1 };
            groupListEditPanelLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            groupListEditPanelLayout.columnWeights = new double[] { 0.1 };
            groupListEditPanelLayout.columnWidths = new int[] { 7 };
            groupListEditPanel.setLayout(groupListEditPanelLayout);
            groupListEditPanel.add(getAddGroupButton(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            groupListEditPanel.add(getRemoveGroupButton(),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        }
        return groupListEditPanel;
    }

    private JButton getAddGroupButton() {
        if (addGroupButton == null) {
            addGroupButton = new JButton();
            addGroupButton.setText("+");
            addGroupButton.addActionListener(this::addGroupButtonActionPerformed);
        }
        return addGroupButton;
    }

    private JButton getRemoveGroupButton() {
        if (removeGroupButton == null) {
            removeGroupButton = new JButton();
            removeGroupButton.setText("-");
            removeGroupButton.addActionListener(this::removeGroupButtonActionPerformed);
        }
        return removeGroupButton;
    }
}
