package gov.nasa.ziggy.services.config;

import java.util.Objects;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "ziggy_KeyValuePair")
public class KeyValuePair {
    @Id
    @Column(name = "keyName", length = 100)
    private String key;
    private String value;

    /**
     * Used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes.
     */
    @Version
    private final int dirty = 0;

    public KeyValuePair() {
    }

    public KeyValuePair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getDirty() {
        return dirty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        KeyValuePair other = (KeyValuePair) obj;
        return Objects.equals(key, other.key) && Objects.equals(value, other.value);
    }
}
