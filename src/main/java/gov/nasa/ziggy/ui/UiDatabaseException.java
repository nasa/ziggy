package gov.nasa.ziggy.ui;

import org.hibernate.Session;

import gov.nasa.ziggy.ui.util.proxy.CrudProxy;

/**
 * This exception is thrown by {@link CrudProxy} methods in response to a Hibernate/database error.
 * It's an indication to calling code that modal dialogs that contain database objects should be
 * closed because the database objects have become transient (because the underlying Hibernate
 * {@link Session} has been closed.
 *
 * @author Todd Klaus
 */
public class UiDatabaseException extends RuntimeException {
    private static final long serialVersionUID = 20230511L;

    public UiDatabaseException() {
    }

    public UiDatabaseException(String message) {
        super(message);
    }

    public UiDatabaseException(Throwable cause) {
        super(cause);
    }

    public UiDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
