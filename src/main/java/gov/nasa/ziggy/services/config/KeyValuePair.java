package gov.nasa.ziggy.services.config;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

@Entity
@Table(name = "PI_KV_PAIR")
public class KeyValuePair {
    @Id
    @Column(name = "keyName", length = 100)
    private String key;
    private String value;

    /**
     * used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes
     */
    @Version
    private final int dirty = 0;

    public KeyValuePair() {
    }

    public KeyValuePair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @return Returns the key.
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key The key to set.
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return Returns the value.
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value The value to set.
     */
    public void setValue(String value) {
        this.value = value;
    }

    public int getDirty() {
        return dirty;
    }
}
