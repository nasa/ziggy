package gov.nasa.ziggy.util;

import gov.nasa.ziggy.parameters.Parameters;

public class TestBean implements Parameters {
    private int anInt = 1;
    private float aFloat = 2.0f;
    private String aString = "Ja Ja Ja";
    private int[] anIntArray = { 1, 2, 3 };
    private float[] aFloatArray = { 1.0f, 2.0f, 3.0f };
    private float[] aFloatArray2 = { 4.0f, 5.0f, 6.0f };
    private String[] aStringArray = { "one", "two", "three" };
    private String[] aStringArrayNull;

    public TestBean() {
    }

    public int getAnInt() {
        return anInt;
    }

    public void setAnInt(int anInt) {
        this.anInt = anInt;
    }

    public float getAFloat() {
        return aFloat;
    }

    public void setAFloat(float float1) {
        aFloat = float1;
    }

    public String getAString() {
        return aString;
    }

    public void setAString(String string) {
        aString = string;
    }

    public int[] getAnIntArray() {
        return anIntArray;
    }

    public void setAnIntArray(int[] anIntArray) {
        this.anIntArray = anIntArray;
    }

    public float[] getAFloatArray() {
        return aFloatArray;
    }

    public void setAFloatArray(float[] floatArray) {
        aFloatArray = floatArray;
    }

    public String[] getAStringArray() {
        return aStringArray;
    }

    public void setAStringArray(String[] stringArray) {
        aStringArray = stringArray;
    }

    public float[] getAFloatArray2() {
        return aFloatArray2;
    }

    public void setAFloatArray2(float[] floatArray2) {
        aFloatArray2 = floatArray2;
    }

    /**
     * @return the aStringArrayNull
     */
    public String[] getAStringArrayNull() {
        return aStringArrayNull;
    }

    /**
     * @param stringArrayNull the aStringArrayNull to set
     */
    public void setAStringArrayNull(String[] stringArrayNull) {
        aStringArrayNull = stringArrayNull;
    }
}
