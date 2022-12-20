package gov.nasa.ziggy.ui.config.security;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.Role;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;

/**
 * @author Todd Klaus
 */
@SuppressWarnings("serial")
public class RolesTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(RolesTableModel.class);

    private List<Role> roles = new LinkedList<>();
    private final UserCrudProxy userCrud;

    public RolesTableModel() {
        userCrud = new UserCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        log.debug("loadFromDatabase() - start");

        if (roles != null) {
            userCrud.evictAll(roles);
        }

        try {
            roles = userCrud.retrieveAllRoles();
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

    public Role getRoleAtRow(int rowIndex) {
        validityCheck();
        return roles.get(rowIndex);
    }

    @Override
    public int getRowCount() {
        validityCheck();
        return roles.size();
    }

    @Override
    public int getColumnCount() {
        return 2;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        Role role = roles.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return role.getName();
            case 1:
                return getPrivilegeList(role);
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
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
        switch (column) {
            case 0:
                return "Role";
            case 1:
                return "Privileges";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
