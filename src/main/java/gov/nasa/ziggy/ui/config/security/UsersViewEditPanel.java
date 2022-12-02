package gov.nasa.ziggy.ui.config.security;

import javax.swing.JOptionPane;
import javax.swing.table.AbstractTableModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.PipelineUIException;
import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import gov.nasa.ziggy.ui.common.MessageUtil;
import gov.nasa.ziggy.ui.config.AbstractViewEditPanel;
import gov.nasa.ziggy.ui.proxy.CrudProxy;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;

@SuppressWarnings("serial")
public class UsersViewEditPanel extends AbstractViewEditPanel {
    private static final Logger log = LoggerFactory.getLogger(UsersViewEditPanel.class);

    private UsersTableModel usersTableModel; // do NOT init to null! (see getTableModel)
    private final UserCrudProxy userCrud;

    public UsersViewEditPanel() throws PipelineUIException {
        super();

        userCrud = new UserCrudProxy();

        initGUI();
    }

    @Override
    protected AbstractTableModel getTableModel() throws PipelineUIException {
        log.debug("getTableModel() - start");

        if (usersTableModel == null) {
            usersTableModel = new UsersTableModel();
            usersTableModel.register();
        }

        log.debug("getTableModel() - end");
        return usersTableModel;
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

        showEditDialog(new User());

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

        showEditDialog(usersTableModel.getUserAtRow(row));

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

        User user = usersTableModel.getUserAtRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete user '" + user.getLoginName() + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                try {
                    userCrud.deleteUser(user);
                } catch (ConsoleSecurityException e) {
                    MessageUtil.showError(this, e);
                }
                usersTableModel.loadFromDatabase();
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }

        log.debug("doDelete(int) - end");
    }

    @Override
    protected void doRefresh() {
        try {
            usersTableModel.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(User user) {
        log.debug("showEditDialog() - start");

        UserEditDialog inst = ZiggyGuiConsole.newUserEditDialog(user);

        log.debug("before visible");
        inst.setVisible(true);
        log.debug("after visible");
        try {
            usersTableModel.loadFromDatabase();
        } catch (Exception e) {
            log.error("showEditDialog(User)", e);

            MessageUtil.showError(this, e);
        }

        log.debug("showEditDialog() - end");
    }

    @Override
    protected String getEditMenuText() {
        return "Edit selected user...";
    }

    @Override
    protected String getNewMenuText() {
        return "Add user...";
    }

    @Override
    protected String getDeleteMenuText() {
        return "Delete selected user...";
    }
}
