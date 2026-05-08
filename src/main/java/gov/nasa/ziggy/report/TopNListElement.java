package gov.nasa.ziggy.report;

/**
 * An element in the top N list with a value and a label.
 *
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TopNListElement {
    private double value;
    private String label;

    public TopNListElement(double value, String label) {
        this.value = value;
        this.label = label;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }
}
