package gov.nasa.ziggy.parameters;

import gov.nasa.ziggy.module.io.Persistable;

public class ParameterTestClasses {

    public static class TestParameters implements Parameters {
        private boolean booleanValue;
        private long longValue;
        private int intValue;
        private int[] intArray;
        private double doubleValue;
        private String stringValue;
        private String[] stringArray;

        public TestParameters() {
        }

        public boolean isBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        public long getLongValue() {
            return longValue;
        }

        public void setLongValue(long longValue) {
            this.longValue = longValue;
        }

        public int getIntValue() {
            return intValue;
        }

        public void setIntValue(int intValue) {
            this.intValue = intValue;
        }

        public int[] getIntArray() {
            return intArray;
        }

        public void setIntArray(int[] intArray) {
            this.intArray = intArray;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(double doubleValue) {
            this.doubleValue = doubleValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        public void setStringValue(String stringValue) {
            this.stringValue = stringValue;
        }

        public String[] getStringArray() {
            return stringArray;
        }

        public void setStringArray(String[] stringArray) {
            this.stringArray = stringArray;
        }
    }

    public static class TestParametersBaz implements Parameters {
        private int baz1;
        private boolean baz2;
        private String baz3;
        private String[] baz4;

        public TestParametersBaz() {
        }

        public int getBaz1() {
            return baz1;
        }

        public void setBaz1(int baz1) {
            this.baz1 = baz1;
        }

        public boolean isBaz2() {
            return baz2;
        }

        public void setBaz2(boolean baz2) {
            this.baz2 = baz2;
        }

        public String getBaz3() {
            return baz3;
        }

        public void setBaz3(String baz3) {
            this.baz3 = baz3;
        }

        public String[] getBaz4() {
            return baz4;
        }

        public void setBaz4(String[] baz4) {
            this.baz4 = baz4;
        }
    }

    public static class TestParametersBar implements Persistable, Parameters {
        private float bar1;
        private double bar2;
        private short bar3;

        public TestParametersBar() {
        }

        public TestParametersBar(float bar1, double bar2, short bar3) {
            this.bar1 = bar1;
            this.bar2 = bar2;
            this.bar3 = bar3;
        }

        public float getBar1() {
            return bar1;
        }

        public void setBar1(float bar1) {
            this.bar1 = bar1;
        }

        public double getBar2() {
            return bar2;
        }

        public void setBar2(double bar2) {
            this.bar2 = bar2;
        }

        public short getBar3() {
            return bar3;
        }

        public void setBar3(short bar3) {
            this.bar3 = bar3;
        }
    }

    public static class TestParametersFoo implements Persistable, Parameters {
        private int foo1;
        private boolean foo2;
        private String foo3;
        private String[] foo4;

        public TestParametersFoo() {
        }

        public TestParametersFoo(int foo1, boolean foo2, String foo3, String[] foo4) {
            this.foo1 = foo1;
            this.foo2 = foo2;
            this.foo3 = foo3;
            this.foo4 = foo4;
        }

        public int getFoo1() {
            return foo1;
        }

        public void setFoo1(int foo1) {
            this.foo1 = foo1;
        }

        public boolean isFoo2() {
            return foo2;
        }

        public void setFoo2(boolean foo2) {
            this.foo2 = foo2;
        }

        public String getFoo3() {
            return foo3;
        }

        public void setFoo3(String foo3) {
            this.foo3 = foo3;
        }

        public String[] getFoo4() {
            return foo4;
        }

        public void setFoo4(String[] foo4) {
            this.foo4 = foo4;
        }
    }

}
