package gov.nasa.ziggy.pipeline.definition;

import gov.nasa.ziggy.parameters.Parameters;

/**
 * @author Todd Klaus
 */
public class TestModuleParameters implements Parameters {
    private int value = 42;

    public TestModuleParameters() {
    }

    public TestModuleParameters(int value) {
        this.value = value;
    }

    /**
     * @return the value
     */
    public int getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(int value) {
        this.value = value;
    }
}
