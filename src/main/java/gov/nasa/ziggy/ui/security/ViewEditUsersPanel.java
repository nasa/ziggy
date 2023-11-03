package gov.nasa.ziggy.ui.security;

import java.util.LinkedList;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.CrudProxy;
import gov.nasa.ziggy.ui.util.proxy.UserCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

@SuppressWarnings("serial")
public class ViewEditUsersPanel extends AbstractViewEditPanel<User> {

    private final UserCrudProxy userCrud;

    public ViewEditUsersPanel() {
        super(new UsersTableModel());
        userCrud = new UserCrudProxy();

        buildComponent();
    }

    @Override
    protected void create() {
        try {
            CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        showEditDialog(new User());
    }

    @Override
    protected void edit(int row) {
        try {
            CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        showEditDialog(ziggyTable.getContentAtViewRow(row));
    }

    @Override
    protected void delete(int row) {
        try {
            CrudProxy.verifyPrivileges(Privilege.USER_ADMIN);
        } catch (ConsoleSecurityException e) {
            MessageUtil.showError(this, e);
            return;
        }

        User user = ziggyTable.getContentAtViewRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete user '" + user.getLoginName() + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                try {
                    userCrud.deleteUser(user);
                } catch (ConsoleSecurityException e) {
                    MessageUtil.showError(this, e);
                }
                ziggyTable.loadFromDatabase();
            } catch (Throwable e) {
                MessageUtil.showError(this, e);
            }
        }
    }

    @Override
    protected void refresh() {
        try {
            ziggyTable.loadFromDatabase();
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    private void showEditDialog(User user) {
        new UserEditDialog(SwingUtilities.getWindowAncestor(this), user).setVisible(true);
        try {
            ziggyTable.loadFromDatabase();
        } catch (Exception e) {
            MessageUtil.showError(this, e);
        }
    }

    private static class UsersTableModel extends AbstractDatabaseModel<User> {

        private static final String[] COLUMN_NAMES = { "Login", "Name" };

        private List<User> users = new LinkedList<>();
        private final UserCrudProxy userCrud;

        public UsersTableModel() {
            userCrud = new UserCrudProxy();
        }

        @Override
        public void loadFromDatabase() {
            try {
                users = userCrud.retrieveAllUsers();
            } catch (ConsoleSecurityException ignore) {
            }

            fireTableDataChanged();
        }

        // TODO Find a use for getUserAtRow or delete
        @SuppressWarnings("unused")
        public User getUserAtRow(int rowIndex) {
            validityCheck();
            return users.get(rowIndex);
        }

        @Override
        public int getRowCount() {
            validityCheck();
            return users.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            validityCheck();

            User user = users.get(rowIndex);

            return switch (columnIndex) {
                case 0 -> user.getLoginName();
                case 1 -> user.getDisplayName();
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        @Override
        public Class<?> getColumnClass(int columnIndex) {
            return String.class;
        }

        @Override
        public String getColumnName(int column) {
            return COLUMN_NAMES[column];
        }

        @Override
        public User getContentAtRow(int row) {
            validityCheck();
            return users.get(row);
        }

        @Override
        public Class<User> tableModelContentClass() {
            return User.class;
        }
    }
}
