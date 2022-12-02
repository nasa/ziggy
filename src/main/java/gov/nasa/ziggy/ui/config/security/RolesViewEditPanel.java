package gov.nasa.ziggy.ui.config.security;

import java.util.LinkedList;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.DoubleListDialog;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.AbstractViewEditPanel;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;

@SuppressWarnings("serial")
public class RolesViewEditPanel extends AbstractViewEditPanel {
    private static final Logger log = LoggerFactory.getLogger(RolesViewEditPanel.class);

    private RolesTableModel rolesTableModel; // do NOT init to null! (see getTableModel)

    private final UserCrudProxy userCrud;

    public RolesViewEditPanel() throws PipelineUIException {
        super();

        userCrud = new UserCrudProxy();

        initGUI();
    }

    @Override
    protected AbstractTableModel getTableModel() throws PipelineUIException {
        log.debug("getTableModel() - start");

        if (rolesTableModel == null) {
            rolesTableModel = new RolesTableModel();
            rolesTableModel.register();
        }

        log.debug("getTableModel() - end");
        return rolesTableModel;
    }

    @Override
    protected void doNew() {
        log.debug("doNew() - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        String roleName = (String) ZiggyGuiConsole.showInputDialog("Enter a name for the new Role",
            "New Role", JOptionPane.QUESTION_MESSAGE, null, null, "");

        if (roleName != null && roleName.length() > 0) {
            showEditDialog(new Role(roleName));
        }

        log.debug("doNew() - end");
    }

    @Override
    protected void doEdit(int row) {
        log.debug("doEdit(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        showEditDialog(rolesTableModel.getRoleAtRow(row));

        log.debug("doEdit(int) - end");
    }

    @Override
    protected void doDelete(int row) {
        log.debug("doDelete(int) - start");

        try {
            CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        Role role = rolesTableModel.getRoleAtRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete role '" + role.getName() + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                try {
                    userCrud.deleteRole(role);
                } catch (ConsoleSecurityException e) {
                    MessageUtil.showError(this, e);
                }

                rolesTableModel.loadFromDatabase();
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }

        log.debug("doDelete(int) - end");
    }

    @Override
    protected void doRefresh() {
        try {
            rolesTableModel.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(Role role) {
        log.debug("showEditDialog() - start");

        try {
            List<String> currentPrivs = role.getPrivileges();
            List<String> availablePrivs = new LinkedList<>();
            for (Privilege priv : Privilege.values()) {
                if (!currentPrivs.contains(priv.toString())) {
                    availablePrivs.add(priv.toString());
                }
            }

            DoubleListDialog<String> privSelectionDialog = ZiggyGuiConsole.newDoubleListDialog(
                "Privileges for Role " + role.getName(), "Available Privileges", availablePrivs,
                "Selected Privileges", currentPrivs);
            privSelectionDialog.setVisible(true);

            if (privSelectionDialog.wasSavePressed()) {
                List<String> selectedPrivs = privSelectionDialog.getSelectedListContents();
                role.setPrivileges(selectedPrivs);
                try {
                    userCrud.saveRole(role);
                } catch (ConsoleSecurityException e) {
                    MessageUtil.showError(this, e);
                }
                rolesTableModel.loadFromDatabase();
            }
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }

        log.debug("showEditDialog() - end");
    }

    @Override
    protected String getEditMenuText() {
        return "Edit selected role...";
    }

    @Override
    protected String getNewMenuText() {
        return "Add role...";
    }

    @Override
    protected String getDeleteMenuText() {
        return "Delete selected role...";
    }
}
