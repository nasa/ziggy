package gov.nasa.ziggy.ui.security;

import java.util.LinkedList;
import java.util.List;

import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.util.DoubleListDialog;
import gov.nasa.ziggy.ui.util.MessageUtil;
import gov.nasa.ziggy.ui.util.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.util.proxy.CrudProxy;
import gov.nasa.ziggy.ui.util.proxy.UserCrudProxy;
import gov.nasa.ziggy.ui.util.table.AbstractViewEditPanel;

@SuppressWarnings("serial")
public class ViewEditRolesPanel extends AbstractViewEditPanel<Role> {

    private final UserCrudProxy userCrud = new UserCrudProxy();

    public ViewEditRolesPanel() {
        super(new RolesTableModel());

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

        String roleName = (String) JOptionPane.showInputDialog(
            SwingUtilities.getWindowAncestor(this), "Enter a name for the new Role", "New Role",
            JOptionPane.QUESTION_MESSAGE, null, null, "");

        if (roleName != null && !roleName.isEmpty()) {
            showEditDialog(new Role(roleName));
        }
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

        Role role = ziggyTable.getContentAtViewRow(row);

        int choice = JOptionPane.showConfirmDialog(this,
            "Are you sure you want to delete role '" + role.getName() + "'?");

        if (choice == JOptionPane.YES_OPTION) {
            try {
                try {
                    userCrud.deleteRole(role);
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

    private void showEditDialog(Role role) {
        try {
            List<String> currentPrivs = role.getPrivileges();
            List<String> availablePrivs = new LinkedList<>();
            for (Privilege priv : Privilege.values()) {
                if (!currentPrivs.contains(priv.toString())) {
                    availablePrivs.add(priv.toString());
                }
            }

            DoubleListDialog<String> privSelectionDialog = new DoubleListDialog<>(
                SwingUtilities.getWindowAncestor(this), "Privileges for Role " + role.getName(),
                "Available Privileges", availablePrivs, "Selected Privileges", currentPrivs);
            privSelectionDialog.setVisible(true);

            if (privSelectionDialog.wasSavePressed()) {
                List<String> selectedPrivs = privSelectionDialog.getSelectedListContents();
                role.setPrivileges(selectedPrivs);
                try {
                    userCrud.saveRole(role);
                } catch (ConsoleSecurityException e) {
                    MessageUtil.showError(this, e);
                }
                ziggyTable.loadFromDatabase();
            }
        } catch (Throwable e) {
            MessageUtil.showError(this, e);
        }
    }

    /**
     * @author Todd Klaus
     */
    private static class RolesTableModel extends AbstractDatabaseModel<Role> {

        private static final String[] COLUMN_NAMES = { "Role", "Privileges" };

        private List<Role> roles = new LinkedList<>();
        private final UserCrudProxy userCrud;

        public RolesTableModel() {
            userCrud = new UserCrudProxy();
        }

        @Override
        public void loadFromDatabase() {
            try {
                roles = userCrud.retrieveAllRoles();
            } catch (ConsoleSecurityException ignore) {
            }

            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            validityCheck();
            return roles.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMN_NAMES.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            validityCheck();

            Role role = roles.get(rowIndex);

            return switch (columnIndex) {
                case 0 -> role.getName();
                case 1 -> getPrivilegeList(role);
                default -> throw new IllegalArgumentException("Unexpected value: " + columnIndex);
            };
        }

        private String getPrivilegeList(Role role) {
            StringBuilder privList = new StringBuilder();
            boolean first = true;

            for (String privilege : role.getPrivileges()) {
                if (!first) {
                    privList.append(", ");
                }
                first = false;
                privList.append(privilege);
            }
            return privList.toString();
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
        public Role getContentAtRow(int row) {
            validityCheck();
            return roles.get(row);
        }

        @Override
        public Class<Role> tableModelContentClass() {
            return Role.class;
        }
    }
}
