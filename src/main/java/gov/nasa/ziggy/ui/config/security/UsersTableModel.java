package gov.nasa.ziggy.ui.config.security;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.ui.ConsoleSecurityException;
import gov.nasa.ziggy.ui.models.AbstractDatabaseModel;
import gov.nasa.ziggy.ui.proxy.UserCrudProxy;

@SuppressWarnings("serial")
public class UsersTableModel extends AbstractDatabaseModel {
    private static final Logger log = LoggerFactory.getLogger(UsersTableModel.class);

    private List<User> users = new LinkedList<>();
    private final UserCrudProxy userCrud;

    public UsersTableModel() {
        userCrud = new UserCrudProxy();
    }

    @Override
    public void loadFromDatabase() {
        log.debug("loadFromDatabase() - start");

        if (users != null) {
            userCrud.evictAll(users);
        }

        try {
            users = userCrud.retrieveAllUsers();
        } catch (ConsoleSecurityException ignore) {
        }

        fireTableDataChanged();

        log.debug("loadFromDatabase() - end");
    }

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
        return 2;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        validityCheck();

        User user = users.get(rowIndex);

        switch (columnIndex) {
            case 0:
                return user.getLoginName();
            case 1:
                return user.getDisplayName();
            default:
                throw new IllegalArgumentException("Unexpected value: " + columnIndex);
        }
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        return String.class;
    }

    @Override
    public String getColumnName(int column) {
        switch (column) {
            case 0:
                return "Login";
            case 1:
                return "Name";
            default:
                throw new IllegalArgumentException("Unexpected value: " + column);
        }
    }
}
