package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import gov.nasa.ziggy.util.WrapperUtils.WrapperCommand;

public class WrapperUtilsTest {

    @Test
    public void testWrapperParameter() {
        assertEquals("wrapper.app.parameter=-Dfoo",
            WrapperUtils.wrapperParameter("wrapper.app.parameter", "-Dfoo"));
    }

    @Test(expected = NullPointerException.class)
    public void testWrapperParameterNullProp() {
        WrapperUtils.wrapperParameter(null, "-Dfoo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrapperParameterEmptyProp() {
        WrapperUtils.wrapperParameter("", "-Dfoo");
    }

    @Test(expected = NullPointerException.class)
    public void testWrapperParameterNullValue() {
        WrapperUtils.wrapperParameter("wrapper.app.parameter", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrapperParameterEmptyValue() {
        WrapperUtils.wrapperParameter("wrapper.app.parameter", "");
    }

    @Test
    public void testIndexedWrapperParameter() {
        assertEquals("wrapper.app.parameter.0=-Dfoo",
            WrapperUtils.wrapperParameter("wrapper.app.parameter.", 0, "-Dfoo"));
        assertEquals("wrapper.app.parameter.1=-Dfoo",
            WrapperUtils.wrapperParameter("wrapper.app.parameter.", 1, "-Dfoo"));
    }

    @Test(expected = NullPointerException.class)
    public void testIndexedWrapperParameterNullProp() {
        WrapperUtils.wrapperParameter(null, 1, "-Dfoo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexedWrapperParameterEmptyProp() {
        WrapperUtils.wrapperParameter("", 1, "-Dfoo");
    }

    @Test(expected = NullPointerException.class)
    public void testIndexedWrapperParameterNullValue() {
        WrapperUtils.wrapperParameter("wrapper.app.parameter", 1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexedWrapperParameterEmptyValue() {
        WrapperUtils.wrapperParameter("wrapper.app.parameter", 1, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexedWrapperParameterNegativeIndex() {
        WrapperUtils.wrapperParameter("wrapper.app.parameter", -1, "-Dfoo");
    }

    @Test
    public void testWrapperCommandEnum() {
        assertEquals("start", WrapperCommand.START.toString());
        assertEquals("stop", WrapperCommand.STOP.toString());
        assertEquals("status", WrapperCommand.STATUS.toString());
    }
}
