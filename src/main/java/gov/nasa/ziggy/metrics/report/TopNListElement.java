package gov.nasa.ziggy.metrics.report;

import java.io.Serializable;

/**
 * @author Todd Klaus
 */
public class TopNListElement implements Serializable {
    private static final long serialVersionUID = 20230511L;

    private long value;
    private String label = null;
    private Object userData = null;

    public TopNListElement(long value, String label) {
        this.value = value;
        this.label = label;
    }

    public TopNListElement(long value, String label, Object userData) {
        this.value = value;
        this.label = label;
        this.userData = userData;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Object getUserData() {
        return userData;
    }

    public void setUserData(Object userData) {
        this.userData = userData;
    }

    @Override
    public String toString() {
        return value + "";
    }
}
