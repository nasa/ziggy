package gov.nasa.ziggy.data.datastore;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlTransient;

/**
 * Models a datastore regular expression ("regexp").
 * <p>
 * A datastore regexp is an element that can be included in multiple datastore nodes. It provides
 * multiple limits on what directory names it will match:
 * <ol>
 * <li>At the top level, the {@link #value} field specifies a "must-meet" regular expression
 * criterion. This allows the user to specify that, for example, datastore regexp foo will only
 * accept values of "bar" or "baz". This regexp can only be changed by re-importing the datastore
 * definitions (i.e., the user can't change it from the console).
 * <li>The user can also set additional include and exclude regexps. These apply additional
 * constraints that can be changed as needed. Their purpose is to allow the datastore API to limit
 * the directories that are used in a specified processing activity. For example, if the user wanted
 * to only process foo directories named "bar", they could either set the include regexp to "bar" or
 * the exclude regexp to "baz".
 * </ol>
 *
 * @author PT
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "Ziggy_DatastoreRegexp")
public class DatastoreRegexp {

    @Id
    @XmlAttribute(required = true)
    private String name;

    @XmlAttribute(required = true)
    private String value;

    @XmlTransient
    private String include;

    @XmlTransient
    private String exclude;

    public DatastoreRegexp() {
    }

    DatastoreRegexp(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public boolean matches(String location) {
        boolean matches = location.matches(value);
        if (matches && !StringUtils.isBlank(include)) {
            matches = matches && location.matches(include);
        }
        if (matches && !StringUtils.isBlank(exclude)) {
            matches = matches && !location.matches(exclude);
        }
        return matches;
    }

    public boolean matchesValue(String location) {
        return location.matches(value);
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    /**
     * Package scoped because only the {@link DatastoreConfigurationImporter} should be able to
     * change this.
     */
    void setValue(String value) {
        this.value = value;
    }

    public String getInclude() {
        return include;
    }

    public void setInclude(String include) {
        this.include = include;
    }

    public String getExclude() {
        return exclude;
    }

    public void setExclude(String exclude) {
        this.exclude = exclude;
    }

    // The hashCode() and equals() methods use only the name, which must be unique per the
    // database uniqueness constraint.
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DatastoreRegexp other = (DatastoreRegexp) obj;
        return Objects.equals(name, other.name);
    }
}
