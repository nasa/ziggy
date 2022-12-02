package gov.nasa.ziggy.services.database;

/**
 * An enumeration of the types of databases supported by Ziggy.
 *
 * @see DatabaseController
 * @author Todd Klaus
 */
public enum SqlDialect {
    HSQLDB("org.hibernate.dialect.HSQLDialect"),
    // ORACLE("org.hibernate.dialect.OracleDialect"),
    POSTGRESQL("org.hibernate.dialect.PostgreSQLDialect");

    private final String dialect;

    SqlDialect(String dstr) {
        dialect = dstr;
    }

    public String dialect() {
        return dialect;
    }
}
