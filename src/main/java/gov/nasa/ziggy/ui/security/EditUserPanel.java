package gov.nasa.ziggy.ui.security;

import static gov.nasa.ziggy.ui.ZiggyGuiConstants.EDIT;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.LinkedList;
import java.util.List;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.util.DoubleListDialog;
import gov.nasa.ziggy.ui.util.GenericListModel;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.ZiggySwingUtils;
import gov.nasa.ziggy.ui.util.proxy.UserCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class EditUserPanel extends javax.swing.JPanel {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(EditUserPanel.class);

    private JLabel loginLabel;
    private JTextField loginTextField;
    private JTextField nameText;
    private JLabel userLabel;
    private JSeparator rolesSep;
    private JButton privsButton;
    private JButton rolesButton;
    private JLabel metaLabel;
    private JList<String> privsList;
    private JScrollPane privsScollPane;
    private JList<Role> rolesList;
    private JScrollPane rolesScrollPane;
    private JPanel actionButtonPanel;
    private JSeparator privsSep;
    private JLabel privsLabel;
    private JLabel rolesLabel;
    private JTextField phoneText;
    private JLabel phoneLabel;
    private JTextField emailText;
    private JLabel emailLabel;
    private JSeparator userSep;
    private JLabel nameLabel;
    private final User user;

    private final UserCrudProxy userCrud;

    public EditUserPanel(User user) {
        this.user = user;
        userCrud = new UserCrudProxy();
        buildComponent();
    }

    public void updateUser() {
        user.setLoginName(loginTextField.getText());
        user.setDisplayName(nameText.getText());
        user.setEmail(emailText.getText());
        user.setPhone(phoneText.getText());
    }

    private void rolesButtonActionPerformed() {
        try {
            List<Role> currentRoles = user.getRoles();
            List<Role> allRoles = userCrud.retrieveAllRoles();
            List<Role> availableRoles = new LinkedList<>();
            for (Role role : allRoles) {
                if (!currentRoles.contains(role)) {
                    availableRoles.add(role);
                }
            }

            DoubleListDialog<Role> roleSelectionDialog = new DoubleListDialog<>(
                SwingUtilities.getWindowAncestor(this), "Roles for " + user.getDisplayName(),
                "Available Roles", availableRoles, "Selected Roles", currentRoles);
            roleSelectionDialog.setVisible(true);

            if (roleSelectionDialog.wasSavePressed()) {
                List<Role> selectedRoles = roleSelectionDialog.getSelectedListContents();
                user.setRoles(selectedRoles);
                rolesList.setModel(new GenericListModel<>(selectedRoles));
            }
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void privsButtonActionPerformed() {
        try {
            List<String> currentPrivs = user.getPrivileges();
            List<String> availablePrivs = new LinkedList<>();
            for (Privilege priv : Privilege.values()) {
                if (!currentPrivs.contains(priv.toString())) {
                    availablePrivs.add(priv.toString());
                }
            }

            DoubleListDialog<String> privSelectionDialog = new DoubleListDialog<>(
                SwingUtilities.getWindowAncestor(this), "Privileges for " + user.getDisplayName(),
                "Available Privileges", availablePrivs, "Selected Privileges", currentPrivs);
            privSelectionDialog.setVisible(true);

            if (privSelectionDialog.wasSavePressed()) {
                List<String> selectedPrivs = privSelectionDialog.getSelectedListContents();
                user.setPrivileges(selectedPrivs);
                privsList.setModel(new GenericListModel<>(selectedPrivs));
            }
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void buildComponent() {

        GridBagLayout layout = new GridBagLayout(); // rows
        layout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
        layout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7 };
        layout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
        layout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
        setLayout(layout);
        setPreferredSize(new Dimension(600, 400));
        add(getLoginLabel(), new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getLoginTextField(),
            new GridBagConstraints(1, 1, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        add(getNameLabel(), new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getNameText(), new GridBagConstraints(5, 1, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        add(getUserLabel(), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getUserSep(), new GridBagConstraints(1, 0, 7, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));

        add(getEmailLabel(), new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getEmailText(), new GridBagConstraints(1, 2, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        add(getPhoneLabel(), new GridBagConstraints(4, 2, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getPhoneText(), new GridBagConstraints(5, 2, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        add(getRolesLabel(), new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getRolesSep(), new GridBagConstraints(1, 3, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        add(getPrivsLabel(), new GridBagConstraints(4, 3, 1, 1, 0.0, 0.0,
            GridBagConstraints.LINE_END, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getPrivsSep(), new GridBagConstraints(5, 3, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
            GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
        add(getRolesScrollPane(), new GridBagConstraints(1, 4, 3, 4, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
        add(getPrivsScollPane(), new GridBagConstraints(5, 4, 3, 4, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
        add(getMetaLabel(), new GridBagConstraints(0, 9, 9, 1, 0.0, 0.0,
            GridBagConstraints.LINE_START, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        add(getRolesButton(), new GridBagConstraints(2, 8, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getPrivsButton(), new GridBagConstraints(6, 8, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        add(getActionButtonPanel(), new GridBagConstraints(2, 8, 5, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
    }

    private JLabel getLoginLabel() {
        if (loginLabel == null) {
            loginLabel = new JLabel();
            loginLabel.setText("Login");
        }

        return loginLabel;
    }

    private JTextField getLoginTextField() {
        if (loginTextField == null) {
            loginTextField = new JTextField();
            loginTextField.setText(user.getLoginName());
        }

        return loginTextField;
    }

    private JLabel getNameLabel() {
        if (nameLabel == null) {
            nameLabel = new JLabel();
            nameLabel.setText("Full Name");
        }

        return nameLabel;
    }

    private JTextField getNameText() {
        if (nameText == null) {
            nameText = new JTextField();
            nameText.setText(user.getDisplayName());
        }

        return nameText;
    }

    private JLabel getUserLabel() {
        if (userLabel == null) {
            userLabel = new JLabel();
            userLabel.setText("User");
            userLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }

        return userLabel;
    }

    private JSeparator getUserSep() {
        if (userSep == null) {
            userSep = new JSeparator();
        }

        return userSep;
    }

    private JLabel getEmailLabel() {
        if (emailLabel == null) {
            emailLabel = new JLabel();
            emailLabel.setText("Email");
        }

        return emailLabel;
    }

    private JTextField getEmailText() {
        if (emailText == null) {
            emailText = new JTextField();
            emailText.setText(user.getEmail());
        }

        return emailText;
    }

    private JLabel getPhoneLabel() {
        if (phoneLabel == null) {
            phoneLabel = new JLabel();
            phoneLabel.setText("Phone");
        }

        return phoneLabel;
    }

    private JTextField getPhoneText() {
        if (phoneText == null) {
            phoneText = new JTextField();
            phoneText.setText(user.getPhone());
        }

        return phoneText;
    }

    private JLabel getRolesLabel() {
        if (rolesLabel == null) {
            rolesLabel = new JLabel();
            rolesLabel.setText("Roles");
            rolesLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }

        return rolesLabel;
    }

    private JSeparator getRolesSep() {
        if (rolesSep == null) {
            rolesSep = new JSeparator();
        }

        return rolesSep;
    }

    private JLabel getPrivsLabel() {
        if (privsLabel == null) {
            privsLabel = new JLabel();
            privsLabel.setText("Privileges");
            privsLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }

        return privsLabel;
    }

    private JSeparator getPrivsSep() {
        if (privsSep == null) {
            privsSep = new JSeparator();
        }

        return privsSep;
    }

    private JScrollPane getRolesScrollPane() {
        if (rolesScrollPane == null) {
            rolesScrollPane = new JScrollPane();
            rolesScrollPane.setViewportView(getRolesList());
        }

        return rolesScrollPane;
    }

    private JList<Role> getRolesList() {
        if (rolesList == null) {
            DefaultListModel<Role> rolesListModel = new DefaultListModel<>();
            for (Role role : user.getRoles()) {
                rolesListModel.addElement(role);
            }
            rolesList = new JList<>();
            rolesList.setModel(rolesListModel);
            rolesList.setVisibleRowCount(3);
        }

        return rolesList;
    }

    private JScrollPane getPrivsScollPane() {
        if (privsScollPane == null) {
            privsScollPane = new JScrollPane();
            privsScollPane.setViewportView(getPrivsList());
        }

        return privsScollPane;
    }

    private JList<String> getPrivsList() {
        if (privsList == null) {
            DefaultListModel<String> privsListModel = new DefaultListModel<>();
            for (String privilege : user.getPrivileges()) {
                privsListModel.addElement(privilege);
            }
            privsList = new JList<>();
            privsList.setModel(privsListModel);
            privsList.setVisibleRowCount(3);
        }

        return privsList;
    }

    private JLabel getMetaLabel() {
        if (metaLabel == null) {
            metaLabel = new JLabel();
            metaLabel.setText("Modified: " + user.getCreated() + " by admin");
            // metaLabel.setText("Modified: 7/1/05 17:55:00 by admin");
            metaLabel.setFont(new java.awt.Font("Dialog", 2, 12));
        }

        return metaLabel;
    }

    private JButton getRolesButton() {
        if (rolesButton == null) {
            rolesButton = new JButton();
            rolesButton.setText(EDIT);
            rolesButton.addActionListener(evt -> {
                rolesButtonActionPerformed();
            });
        }

        return rolesButton;
    }

    private JButton getPrivsButton() {
        if (privsButton == null) {
            privsButton = new JButton();
            privsButton.setText(EDIT);
            privsButton.addActionListener(evt -> {
                privsButtonActionPerformed();
            });
        }

        return privsButton;
    }

    private JPanel getActionButtonPanel() {
        if (actionButtonPanel == null) {
            actionButtonPanel = new JPanel();
            FlowLayout actionButtonPanelLayout = new FlowLayout();
            actionButtonPanelLayout.setHgap(35);
            actionButtonPanel.setLayout(actionButtonPanelLayout);
        }

        return actionButtonPanel;
    }

    public static void main(String[] args) {
        User newUser = new User("user1", "User One", "user1@example.com", "555-0100");
        Role r1 = new Role("role1");
        r1.addPrivilege(Privilege.PIPELINE_OPERATIONS.toString());
        r1.addPrivilege(Privilege.PIPELINE_MONITOR.toString());
        Role r2 = new Role("role2");
        r2.addPrivilege(Privilege.PIPELINE_OPERATIONS.toString());
        r2.addPrivilege(Privilege.PIPELINE_MONITOR.toString());
        newUser.addRole(r1);
        newUser.addRole(r2);

        ZiggySwingUtils.displayTestDialog(new EditUserPanel(newUser));
    }
}
