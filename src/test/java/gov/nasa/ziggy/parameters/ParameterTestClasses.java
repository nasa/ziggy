package gov.nasa.ziggy.parameters;

import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * Test parameter classes.
 * <p>
 * Note that these classes were originally written for the Spiffy parameter API, in which parameter
 * classes conformed to the design rules for Java Beans and used bean utilities to manage the
 * interchange between the database and the Java objects. The Ziggy parameter API uses
 * {@link TypedParameter} instances as the definitive representation of the parameter values, and
 * the parameter fields are populated from the TypedParameter collection but the TypedParameter
 * collection never gets populated from the parameter fields. In general this relies on the
 * parameters being imported from XML and then saved to the database, in which case the management
 * of the two representations is automagically handled. For these test classes, the parameter values
 * don't get imported from XML and the class instances are often constructed rather than retrieved,
 * so it was necessary to modify the classes such that the TypedParameter collection is correctly
 * constructed and populated, and such that the field setters automatically propagate new values to
 * the TypedParameter collection.
 *
 * @author PT
 */
public class ParameterTestClasses {

    public static class TestParameters extends Parameters {
        private boolean booleanValue;
        private long longValue;
        private int intValue;
        private int[] intArray;
        private double doubleValue;
        private String stringValue;
        private String[] stringArray;

        public TestParameters() {
            addParameter(
                new TypedParameter("booleanValue", "false", ZiggyDataType.ZIGGY_BOOLEAN, true));
            addParameter(new TypedParameter("longValue", "0", ZiggyDataType.ZIGGY_LONG, true));
            addParameter(new TypedParameter("intValue", "0", ZiggyDataType.ZIGGY_INT, true));
            addParameter(new TypedParameter("intArray", "", ZiggyDataType.ZIGGY_INT, false));
            addParameter(new TypedParameter("doubleValue", "0", ZiggyDataType.ZIGGY_DOUBLE, true));
            addParameter(new TypedParameter("stringValue", "", ZiggyDataType.ZIGGY_STRING, true));
            addParameter(new TypedParameter("stringArray", "", ZiggyDataType.ZIGGY_STRING, false));
        }

        public boolean isBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(boolean booleanValue) {
            updateParameter("booleanValue", ZiggyDataType.objectToString(booleanValue));
        }

        public long getLongValue() {
            return longValue;
        }

        public void setLongValue(long longValue) {
            updateParameter("longValue", ZiggyDataType.objectToString(longValue));
        }

        public int getIntValue() {
            return intValue;
        }

        public void setIntValue(int intValue) {
            updateParameter("intValue", ZiggyDataType.objectToString(intValue));
        }

        public int[] getIntArray() {
            return intArray;
        }

        public void setIntArray(int[] intArray) {
            updateParameter("intArray", ZiggyDataType.objectToString(intArray));
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public void setDoubleValue(double doubleValue) {
            updateParameter("doubleValue", ZiggyDataType.objectToString(doubleValue));
        }

        public String getStringValue() {
            return stringValue;
        }

        public void setStringValue(String stringValue) {
            updateParameter("stringValue", ZiggyDataType.objectToString(stringValue));
        }

        public String[] getStringArray() {
            return stringArray;
        }

        public void setStringArray(String[] stringArray) {
            updateParameter("stringArray", ZiggyDataType.objectToString(stringArray));
        }
    }

    public static class TestParametersBaz extends Parameters {
        private int baz1;
        private boolean baz2;
        private String baz3;
        private String[] baz4;

        public TestParametersBaz() {
            addParameter(new TypedParameter("baz1", "0", ZiggyDataType.ZIGGY_INT, true));
            addParameter(new TypedParameter("baz2", "false", ZiggyDataType.ZIGGY_BOOLEAN, true));
            addParameter(new TypedParameter("baz3", "", ZiggyDataType.ZIGGY_STRING, true));
            addParameter(new TypedParameter("baz4", "", ZiggyDataType.ZIGGY_STRING, false));
        }

        public int getBaz1() {
            return baz1;
        }

        public void setBaz1(int baz1) {
            updateParameter("baz1", ZiggyDataType.objectToString(baz1));
        }

        public boolean isBaz2() {
            return baz2;
        }

        public void setBaz2(boolean baz2) {
            updateParameter("baz2", ZiggyDataType.objectToString(baz2));
        }

        public String getBaz3() {
            return baz3;
        }

        public void setBaz3(String baz3) {
            updateParameter("baz3", ZiggyDataType.objectToString(baz3));
        }

        public String[] getBaz4() {
            return baz4;
        }

        public void setBaz4(String[] baz4) {
            updateParameter("baz4", ZiggyDataType.objectToString(baz4));
        }
    }

    public static class TestParametersBar extends Parameters implements Persistable {
        private float bar1;
        private double bar2;
        private short bar3;

        public TestParametersBar() {
            addParameter(new TypedParameter("bar1", "0", ZiggyDataType.ZIGGY_FLOAT, true));
            addParameter(new TypedParameter("bar2", "0", ZiggyDataType.ZIGGY_DOUBLE, true));
            addParameter(new TypedParameter("bar3", "0", ZiggyDataType.ZIGGY_SHORT, true));
        }

        public TestParametersBar(float bar1, double bar2, short bar3) {
            this();
            setBar1(bar1);
            setBar2(bar2);
            setBar3(bar3);
        }

        public float getBar1() {
            return bar1;
        }

        public void setBar1(float bar1) {
            updateParameter("bar1", ZiggyDataType.objectToString(bar1));
        }

        public double getBar2() {
            return bar2;
        }

        public void setBar2(double bar2) {
            updateParameter("bar2", ZiggyDataType.objectToString(bar2));
        }

        public short getBar3() {
            return bar3;
        }

        public void setBar3(short bar3) {
            updateParameter("bar3", ZiggyDataType.objectToString(bar3));
        }
    }

    public static class TestParametersFoo extends Parameters implements Persistable {
        private int foo1;
        private boolean foo2;
        private String foo3;
        private String[] foo4;

        public TestParametersFoo() {
            addParameter(new TypedParameter("foo1", "0", ZiggyDataType.ZIGGY_INT, true));
            addParameter(new TypedParameter("foo2", "0", ZiggyDataType.ZIGGY_BOOLEAN, true));
            addParameter(new TypedParameter("foo3", "", ZiggyDataType.ZIGGY_STRING, true));
            addParameter(new TypedParameter("foo4", "", ZiggyDataType.ZIGGY_STRING, false));
        }

        public TestParametersFoo(int foo1, boolean foo2, String foo3, String[] foo4) {
            this();
            setFoo1(foo1);
            setFoo2(foo2);
            setFoo3(foo3);
            setFoo4(foo4);
        }

        public int getFoo1() {
            return foo1;
        }

        public void setFoo1(int foo1) {
            updateParameter("foo1", ZiggyDataType.objectToString(foo1));
        }

        public boolean isFoo2() {
            return foo2;
        }

        public void setFoo2(boolean foo2) {
            updateParameter("foo2", ZiggyDataType.objectToString(foo2));
        }

        public String getFoo3() {
            return foo3;
        }

        public void setFoo3(String foo3) {
            updateParameter("foo3", ZiggyDataType.objectToString(foo3));
        }

        public String[] getFoo4() {
            return foo4;
        }

        public void setFoo4(String[] foo4) {
            updateParameter("foo4", ZiggyDataType.objectToString(foo4));
        }
    }
}
