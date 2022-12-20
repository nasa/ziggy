package gov.nasa.ziggy.pipeline.definition;

import gov.nasa.ziggy.parameters.Parameters;

/**
 * @author Todd Klaus
 */
public class TestBean2 implements Parameters {
    private String a = "a";
    private String b = "b";

    public TestBean2(String a, String b) {
        this.a = a;
        this.b = b;
    }

    public TestBean2() {
    }

    /**
     * @return the a
     */
    public String getA() {
        return a;
    }

    /**
     * @param a the a to set
     */
    public void setA(String a) {
        this.a = a;
    }

    /**
     * @return the b
     */
    public String getB() {
        return b;
    }

    /**
     * @param b the b to set
     */
    public void setB(String b) {
        this.b = b;
    }
}
