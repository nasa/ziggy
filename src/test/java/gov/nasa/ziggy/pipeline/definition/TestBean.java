package gov.nasa.ziggy.pipeline.definition;

import gov.nasa.ziggy.parameters.Parameters;

/**
 * @author Todd Klaus
 */
public class TestBean implements Parameters {
    private int a = 1;
    private String b = "foo";
    private int[] c = { 1, 2, 3 };
    private String[] d = { "a", "b", "c" };

    public TestBean(int a, String b, int[] c, String[] d) {
        super();
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }

    public TestBean() {
    }

    /**
     * @return the a
     */
    public int getA() {
        return a;
    }

    /**
     * @param a the a to set
     */
    public void setA(int a) {
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

    public int[] getC() {
        return c;
    }

    public void setC(int[] c) {
        this.c = c;
    }

    public String[] getD() {
        return d;
    }

    public void setD(String[] d) {
        this.d = d;
    }
}
