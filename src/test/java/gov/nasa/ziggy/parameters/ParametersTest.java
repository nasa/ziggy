package gov.nasa.ziggy.parameters;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_INT;
import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_STRING;
import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.TypedParameter;

/**
 * @author Bill Wohler
 */
public class ParametersTest {

    @Test
    public void testPopulate() throws Exception {
        Set<TypedParameter> typedParameters = Set.of(new TypedParameter("a", "42", ZIGGY_INT),
            new TypedParameter("b", "xyzzy", ZIGGY_STRING),
            new TypedParameter("c", "1,3,5,7,11", ZIGGY_INT, false),
            new TypedParameter("d", "a,b,c", ZIGGY_STRING, false));

        TestBean bean = TestBean.class.getDeclaredConstructor().newInstance();
        bean.populate(typedParameters);

        TestBean expectedBean = new TestBean(42, "xyzzy", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "a", "b", "c" });

        assertEquals(expectedBean, bean);
    }
}
