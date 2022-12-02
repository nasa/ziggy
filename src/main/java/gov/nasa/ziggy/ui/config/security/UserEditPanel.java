package gov.nasa.ziggy.ui.config.security;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.LinkedList;
import java.util.List;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.DoubleListDialog;
import gov.nasa.ziggy.ui.common.GenericListModel;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class UserEditPanel extends javax.swing.JPanel {
    private static final Logger log = LoggerFactory.getLogger(UserEditPanel.class);

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

    public UserEditPanel(User user) {
        super();
        this.user = user;
        userCrud = new UserCrudProxy();
        initGUI();
    }

    public UserEditPanel() {
        super();
        user = new User();
        userCrud = new UserCrudProxy();
        initGUI();
    }

    public void updateUser() {
        log.debug("updateUser() - start");

        user.setLoginName(loginTextField.getText());
        user.setDisplayName(nameText.getText());
        user.setEmail(emailText.getText());
        user.setPhone(phoneText.getText());

        log.debug("updateUser() - end");
    }

    private void rolesButtonActionPerformed() {
        log.debug("rolesButtonActionPerformed(ActionEvent) - start");
        try {
            List<Role> currentRoles = user.getRoles();
            List<Role> allRoles = userCrud.retrieveAllRoles();
            List<Role> availableRoles = new LinkedList<>();
            for (Role role : allRoles) {
                if (!currentRoles.contains(role)) {
                    availableRoles.add(role);
                }
            }

            DoubleListDialog<Role> roleSelectionDialog = ZiggyGuiConsole.newDoubleListDialog(
                "Roles for " + user.getDisplayName(), "Available Roles", availableRoles,
                "Selected Roles", currentRoles);
            roleSelectionDialog.setVisible(true);

            if (roleSelectionDialog.wasSavePressed()) {
                List<Role> selectedRoles = roleSelectionDialog.getSelectedListContents();
                user.setRoles(selectedRoles);
                rolesList.setModel(new GenericListModel<>(selectedRoles));
            }
        } catch (Throwable e) {
            log.warn("rolesButtonActionPerformed(ActionEvent)", e);

            MessageUtil.showError(this, e);
        }

        log.debug("rolesButtonActionPerformed(ActionEvent) - end");
    }

    private void privsButtonActionPerformed() {
        log.debug("privsButtonActionPerformed(ActionEvent) - start");

        try {
            List<String> currentPrivs = user.getPrivileges();
            List<String> availablePrivs = new LinkedList<>();
            for (Privilege priv : Privilege.values()) {
                if (!currentPrivs.contains(priv.toString())) {
                    availablePrivs.add(priv.toString());
                }
            }

            DoubleListDialog<String> privSelectionDialog = ZiggyGuiConsole.newDoubleListDialog(
                "Privileges for " + user.getDisplayName(), "Available Privileges", availablePrivs,
                "Selected Privileges", currentPrivs);
            privSelectionDialog.setVisible(true);

            if (privSelectionDialog.wasSavePressed()) {
                List<String> selectedPrivs = privSelectionDialog.getSelectedListContents();
                user.setPrivileges(selectedPrivs);
                privsList.setModel(new GenericListModel<>(selectedPrivs));
            }
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }

        log.debug("privsButtonActionPerformed(ActionEvent) - end");
    }

    private void initGUI() {
        log.debug("initGUI() - start");

        try {
            GridBagLayout thisLayout = new GridBagLayout(); // rows
            thisLayout.columnWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1 };
            thisLayout.columnWidths = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            thisLayout.rowWeights = new double[] { 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
                0.1 };
            thisLayout.rowHeights = new int[] { 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7 };
            setLayout(thisLayout);
            setPreferredSize(new Dimension(600, 400));
            this.add(getLoginLabel(),
                new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getLoginTextField(),
                new GridBagConstraints(1, 1, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getNameLabel(),
                new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getNameText(),
                new GridBagConstraints(5, 1, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getUserLabel(),
                new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getUserSep(),
                new GridBagConstraints(1, 0, 7, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));

            this.add(getEmailLabel(),
                new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getEmailText(),
                new GridBagConstraints(1, 2, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getPhoneLabel(),
                new GridBagConstraints(4, 2, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getPhoneText(),
                new GridBagConstraints(5, 2, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getRolesLabel(),
                new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getRolesSep(),
                new GridBagConstraints(1, 3, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getPrivsLabel(),
                new GridBagConstraints(4, 3, 1, 1, 0.0, 0.0, GridBagConstraints.LINE_END,
                    GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getPrivsSep(),
                new GridBagConstraints(5, 3, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getRolesScrollPane(), new GridBagConstraints(1, 4, 3, 4, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getPrivsScollPane(), new GridBagConstraints(5, 4, 3, 4, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getMetaLabel(),
                new GridBagConstraints(0, 9, 9, 1, 0.0, 0.0, GridBagConstraints.LINE_START,
                    GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
            this.add(getRolesButton(), new GridBagConstraints(2, 8, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getPrivsButton(), new GridBagConstraints(6, 8, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
            this.add(getActionButtonPanel(), new GridBagConstraints(2, 8, 5, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        } catch (Exception e) {
            log.error("initGUI()", e);

            e.printStackTrace();
        }

        log.debug("initGUI() - end");
    }

    private JLabel getLoginLabel() {
        log.debug("getLoginLabel() - start");

        if (loginLabel == null) {
            loginLabel = new JLabel();
            loginLabel.setText("Login");
        }

        log.debug("getLoginLabel() - end");
        return loginLabel;
    }

    private JTextField getLoginTextField() {
        log.debug("getLoginTextField() - start");

        if (loginTextField == null) {
            loginTextField = new JTextField();
            loginTextField.setText(user.getLoginName());
        }

        log.debug("getLoginTextField() - end");
        return loginTextField;
    }

    private JLabel getNameLabel() {
        log.debug("getNameLabel() - start");

        if (nameLabel == null) {
            nameLabel = new JLabel();
            nameLabel.setText("Full Name");
        }

        log.debug("getNameLabel() - end");
        return nameLabel;
    }

    private JTextField getNameText() {
        log.debug("getNameText() - start");

        if (nameText == null) {
            nameText = new JTextField();
            nameText.setText(user.getDisplayName());
        }

        log.debug("getNameText() - end");
        return nameText;
    }

    private JLabel getUserLabel() {
        log.debug("getUserLabel() - start");

        if (userLabel == null) {
            userLabel = new JLabel();
            userLabel.setText("User");
            userLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }

        log.debug("getUserLabel() - end");
        return userLabel;
    }

    private JSeparator getUserSep() {
        log.debug("getUserSep() - start");

        if (userSep == null) {
            userSep = new JSeparator();
        }

        log.debug("getUserSep() - end");
        return userSep;
    }

    private JLabel getEmailLabel() {
        log.debug("getEmailLabel() - start");

        if (emailLabel == null) {
            emailLabel = new JLabel();
            emailLabel.setText("Email");
        }

        log.debug("getEmailLabel() - end");
        return emailLabel;
    }

    private JTextField getEmailText() {
        log.debug("getEmailText() - start");

        if (emailText == null) {
            emailText = new JTextField();
            emailText.setText(user.getEmail());
        }

        log.debug("getEmailText() - end");
        return emailText;
    }

    private JLabel getPhoneLabel() {
        log.debug("getPhoneLabel() - start");

        if (phoneLabel == null) {
            phoneLabel = new JLabel();
            phoneLabel.setText("Phone");
        }

        log.debug("getPhoneLabel() - end");
        return phoneLabel;
    }

    private JTextField getPhoneText() {
        log.debug("getPhoneText() - start");

        if (phoneText == null) {
            phoneText = new JTextField();
            phoneText.setText(user.getPhone());
        }

        log.debug("getPhoneText() - end");
        return phoneText;
    }

    private JLabel getRolesLabel() {
        log.debug("getRolesLabel() - start");

        if (rolesLabel == null) {
            rolesLabel = new JLabel();
            rolesLabel.setText("Roles");
            rolesLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }

        log.debug("getRolesLabel() - end");
        return rolesLabel;
    }

    private JSeparator getRolesSep() {
        log.debug("getRolesSep() - start");

        if (rolesSep == null) {
            rolesSep = new JSeparator();
        }

        log.debug("getRolesSep() - end");
        return rolesSep;
    }

    private JLabel getPrivsLabel() {
        log.debug("getPrivsLabel() - start");

        if (privsLabel == null) {
            privsLabel = new JLabel();
            privsLabel.setText("Privileges");
            privsLabel.setFont(new java.awt.Font("Dialog", 1, 14));
        }

        log.debug("getPrivsLabel() - end");
        return privsLabel;
    }

    private JSeparator getPrivsSep() {
        log.debug("getPrivsSep() - start");

        if (privsSep == null) {
            privsSep = new JSeparator();
        }

        log.debug("getPrivsSep() - end");
        return privsSep;
    }

    private JScrollPane getRolesScrollPane() {
        log.debug("getRolesScrollPane() - start");

        if (rolesScrollPane == null) {
            rolesScrollPane = new JScrollPane();
            rolesScrollPane.setViewportView(getRolesList());
        }

        log.debug("getRolesScrollPane() - end");
        return rolesScrollPane;
    }

    private JList<Role> getRolesList() {
        log.debug("getRolesList() - start");

        if (rolesList == null) {
            DefaultListModel<Role> rolesListModel = new DefaultListModel<>();
            for (Role role : user.getRoles()) {
                rolesListModel.addElement(role);
            }
            rolesList = new JList<>();
            rolesList.setModel(rolesListModel);
            rolesList.setVisibleRowCount(3);
        }

        log.debug("getRolesList() - end");
        return rolesList;
    }

    private JScrollPane getPrivsScollPane() {
        log.debug("getPrivsScollPane() - start");

        if (privsScollPane == null) {
            privsScollPane = new JScrollPane();
            privsScollPane.setViewportView(getPrivsList());
        }

        log.debug("getPrivsScollPane() - end");
        return privsScollPane;
    }

    private JList<String> getPrivsList() {
        log.debug("getPrivsList() - start");

        if (privsList == null) {
            DefaultListModel<String> privsListModel = new DefaultListModel<>();
            for (String privilege : user.getPrivileges()) {
                privsListModel.addElement(privilege);
            }
            privsList = new JList<>();
            privsList.setModel(privsListModel);
            privsList.setVisibleRowCount(3);
        }

        log.debug("getPrivsList() - end");
        return privsList;
    }

    private JLabel getMetaLabel() {
        log.debug("getMetaLabel() - start");

        if (metaLabel == null) {
            metaLabel = new JLabel();
            metaLabel.setText("Modified: " + user.getCreated() + " by admin");
            // metaLabel.setText("Modified: 7/1/05 17:55:00 by admin");
            metaLabel.setFont(new java.awt.Font("Dialog", 2, 12));
        }

        log.debug("getMetaLabel() - end");
        return metaLabel;
    }

    private JButton getRolesButton() {
        log.debug("getRolesButton() - start");

        if (rolesButton == null) {
            rolesButton = new JButton();
            rolesButton.setText("edit");
            rolesButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                rolesButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getRolesButton() - end");
        return rolesButton;
    }

    private JButton getPrivsButton() {
        log.debug("getPrivsButton() - start");

        if (privsButton == null) {
            privsButton = new JButton();
            privsButton.setText("edit");
            privsButton.addActionListener(evt -> {
                log.debug("actionPerformed(ActionEvent) - start");

                privsButtonActionPerformed();

                log.debug("actionPerformed(ActionEvent) - end");
            });
        }

        log.debug("getPrivsButton() - end");
        return privsButton;
    }

    private JPanel getActionButtonPanel() {
        log.debug("getActionButtonPanel() - start");

        if (actionButtonPanel == null) {
            actionButtonPanel = new JPanel();
            FlowLayout actionButtonPanelLayout = new FlowLayout();
            actionButtonPanelLayout.setHgap(35);
            actionButtonPanel.setLayout(actionButtonPanelLayout);
        }

        log.debug("getActionButtonPanel() - end");
        return actionButtonPanel;
    }

    /**
     * Auto-generated main method to display this JPanel inside a new JFrame.
     */
    public static void main(String[] args) {
        log.debug("main(String[]) - start");

        JFrame frame = new JFrame();
        User newUser = new User("user1", "User One", "user1@example.com", "555-0100");
        Role r1 = new Role("role1");
        r1.addPrivilege(Privilege.PIPELINE_OPERATIONS.toString());
        r1.addPrivilege(Privilege.PIPELINE_MONITOR.toString());
        Role r2 = new Role("role2");
        r2.addPrivilege(Privilege.PIPELINE_OPERATIONS.toString());
        r2.addPrivilege(Privilege.PIPELINE_MONITOR.toString());
        newUser.addRole(r1);
        newUser.addRole(r2);

        frame.getContentPane().add(new UserEditPanel(newUser));
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);

        log.debug("main(String[]) - end");
    }
}
