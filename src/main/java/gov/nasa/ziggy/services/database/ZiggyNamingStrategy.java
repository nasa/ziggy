package gov.nasa.ziggy.services.database;

import javax.persistence.Column;
import javax.persistence.Table;

import org.hibernate.AssertionFailure;
import org.hibernate.cfg.NamingStrategy;
import org.hibernate.internal.util.StringHelper;

/**
 * This class implements {@link org.hibernate.cfg.NamingStrategy} for the database naming
 * conventions defined for the Ziggy project. Essentially, this consists of converting the Java
 * camel-case names to an all-caps with underscores format.
 * <p>
 * This code is based on {@link org.hibernate.cfg.ImprovedNamingStrategy}, the main difference being
 * that names that are explicitly defined in the code annotations (like {@link Table},
 * {@link Column}, etc.) are not modified.
 *
 * @author Todd Klaus
 */
public class ZiggyNamingStrategy implements NamingStrategy {
    /**
     * A convenient singleton instance
     */
    public static final NamingStrategy INSTANCE = new ZiggyNamingStrategy();

    /**
     * Return the unqualified class name, mixed case converted to underscores
     */
    @Override
    public String classToTableName(String className) {
        return addUnderscores(StringHelper.unqualify(className));
    }

    /**
     * Return the full property path with underscore seperators, mixed case converted to underscores
     */
    @Override
    public String propertyToColumnName(String propertyName) {
        return addUnderscores(StringHelper.unqualify(propertyName));
    }

    /**
     * This method is called for names explicitly defined in the annotations. Leave the name alone!
     */
    @Override
    public String tableName(String tableName) {
        return tableName;
    }

    /**
     * This method is called for names explicitly defined in the annotations. Leave the name alone!
     */
    @Override
    public String columnName(String columnName) {
        return columnName;
    }

    @Override
    public String collectionTableName(String ownerEntity, String ownerEntityTable,
        String associatedEntity, String associatedEntityTable, String propertyName) {
        return tableName(ownerEntityTable + '_' + propertyToColumnName(propertyName));
    }

    /**
     * Return the argument
     */
    @Override
    public String joinKeyColumnName(String joinedColumn, String joinedTable) {
        return addUnderscores(joinedTable) + "_" + addUnderscores(joinedColumn);
    }

    /**
     * Return the property name or propertyTableName
     */
    @Override
    public String foreignKeyColumnName(String propertyName, String propertyEntityName,
        String propertyTableName, String referencedColumnName) {
        String header = propertyName != null ? StringHelper.unqualify(propertyName)
            : propertyTableName;
        if (header == null) {
            throw new AssertionFailure("NamingStrategy not properly filled");
        }
        return addUnderscores(propertyTableName) + "_" + addUnderscores(referencedColumnName);
    }

    /**
     * Return the column name or the unqualified property name
     */
    @Override
    public String logicalColumnName(String columnName, String propertyName) {
        return StringHelper.isNotEmpty(columnName) ? columnName
            : StringHelper.unqualify(propertyName);
    }

    /**
     * Returns either the table name if explicit or if there is an associated table, the
     * concatenation of owner entity table and associated table otherwise the concatenation of owner
     * entity table and the unqualified property name
     */
    @Override
    public String logicalCollectionTableName(String tableName, String ownerEntityTable,
        String associatedEntityTable, String propertyName) {
        if (tableName != null) {
            return tableName;
        }
        // use of a stringbuffer to workaround a JDK bug
        return new StringBuffer(ownerEntityTable).append("_")
            .append(associatedEntityTable != null ? associatedEntityTable
                : StringHelper.unqualify(propertyName))
            .toString();
    }

    /**
     * Return the column name if explicit or the concatenation of the property name and the
     * referenced column
     */
    @Override
    public String logicalCollectionColumnName(String columnName, String propertyName,
        String referencedColumn) {
        return StringHelper.isNotEmpty(columnName) ? columnName
            : StringHelper.unqualify(propertyName) + "_" + referencedColumn;
    }

    /**
     * Convert camel-case to caps and underscores
     *
     * @param name
     * @return
     */
    protected static String addUnderscores(String name) {
        StringBuilder buf = new StringBuilder(name.replace('.', '_'));
        for (int i = 1; i < buf.length() - 1; i++) {
            if (Character.isLowerCase(buf.charAt(i - 1)) && Character.isUpperCase(buf.charAt(i))
                && Character.isLowerCase(buf.charAt(i + 1))) {
                buf.insert(i++, '_');
            }
        }
        return buf.toString().toUpperCase();
    }
}
