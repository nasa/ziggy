package gov.nasa.ziggy.parameters;

import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * @author Todd Klaus
 * @author Bill Wohler
 */
public class TestBean extends Parameters {
    private int a = 1;
    private String b = "foo";
    private int[] c = { 1, 2, 3 };
    private String[] d = { "a", "b", "c" };

    public TestBean(int a, String b, int[] c, String[] d) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }

    public TestBean() {
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Arrays.hashCode(c);
        result = prime * result + Arrays.hashCode(d);
        return prime * result + Objects.hash(a, b);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || getClass() != obj.getClass()) {
            return false;
        }
        TestBean other = (TestBean) obj;
        return a == other.a && Objects.equals(b, other.b) && Arrays.equals(c, other.c)
            && Arrays.equals(d, other.d);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
