package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.module.PipelineException;

/**
 * @author Todd Klaus
 */
public class BeanWrapperTest {

    @Test
    public void constructFromClass() throws Exception {
        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>(TestBean.class);

        Map<String, String> expectedProps = new HashMap<>();
        expectedProps.put("a", "1");
        expectedProps.put("b", "foo");
        expectedProps.put("c", "1,2,3");
        expectedProps.put("d", "a,b,c");

        assertEquals("BeanWrapper<TestBean>.beanClassName", TestBean.class, beanWrapper.getClazz());
        assertEquals("BeanWrapper<TestBean>.props", expectedProps,
            BeanWrapper.propertyValueByName(beanWrapper.getTypedProperties()));

        TestBean expectedBean = new TestBean();
        TestBean actualBean = beanWrapper.getInstance();

        ReflectionEquals comparator = new ReflectionEquals();
        comparator.assertEquals(expectedBean, actualBean);
    }

    @Test
    public void constructFromInstance() throws PipelineException {
        TestBean bean = new TestBean(42, "xyzzy", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "latest--planet.txt", "b.1", "c.1" });
        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>(bean);

        assertEquals("BeanWrapper<TestBean>.beanClassName", TestBean.class, beanWrapper.getClazz());
        assertEquals("BeanWrapper<TestBean>.props.size", 4,
            beanWrapper.getTypedProperties().size());
        Map<String, String> properties = BeanWrapper
            .propertyValueByName(beanWrapper.getTypedProperties());
        assertTrue("BeanWrapper<TestBean>.props contains 'a'", properties.containsKey("a"));
        assertEquals("BeanWrapper<TestBean>.props.get(\"a\")", "42", properties.get("a"));
        assertTrue("BeanWrapper<TestBean>.props contains 'b'", properties.containsKey("b"));
        assertEquals("BeanWrapper<TestBean>.props.get(\"b\")", "xyzzy", properties.get("b"));
        assertTrue("BeanWrapper<TestBean>.props contains 'c'", properties.containsKey("c"));
        assertEquals("BeanWrapper<TestBean>.props.get(\"c\")", "1,3,5,7,11", properties.get("c"));
        assertEquals("BeanWrapper<TestBean>.props.get(\"d\")", "latest--planet.txt,b.1,c.1",
            properties.get("d"));
    }

    @Test
    public void copyConstructor() throws Exception {
        TestBean bean1 = new TestBean(42, "xyzzy", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "a", "b", "c" });
        BeanWrapper<TestBean> beanWrapper1 = new BeanWrapper<>(bean1);
        BeanWrapper<TestBean> beanWrapper2 = new BeanWrapper<>(beanWrapper1);
        TestBean bean2 = beanWrapper2.getInstance();

        ReflectionEquals comparator = new ReflectionEquals();
        comparator.assertEquals(bean1, bean2);

        assertEquals("BeanWrapper<TestBean>.beanClassName", beanWrapper1.getClazz(),
            beanWrapper2.getClazz());
        assertEquals("BeanWrapper<TestBean>.props",
            BeanWrapper.propertyValueByName(beanWrapper1.getTypedProperties()),
            BeanWrapper.propertyValueByName(beanWrapper2.getTypedProperties()));
    }

    @Test
    public void testGetPropertiesAllProvided() {
        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>(TestBean.class);

        Map<String, String> expectedProps = new HashMap<>();
        expectedProps.put("a", "1");
        expectedProps.put("b", "foo");
        expectedProps.put("c", "1,2,3");
        expectedProps.put("d", "a,b,c");

        beanWrapper.setProperties(expectedProps);
        Map<String, String> actualProps = BeanWrapper
            .propertyValueByName(beanWrapper.getTypedProperties());
        assertEquals(expectedProps, actualProps);
    }

    /**
     * Simulate construction of the object after loading from the db.
     *
     * @throws Exception
     */
    @Test
    public void constructFromHibernateLoad() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("a", "42");
        props.put("b", "xyzzy");
        props.put("c", "1,3,5,7,11");
        props.put("d", "a--.1,b_.1,c.1");

        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>();
        beanWrapper.setClazz(TestBean.class);
        beanWrapper.setProperties(props);

        TestBean expectedBean = new TestBean(42, "xyzzy", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "a--.1", "b_.1", "c.1" });
        TestBean actualBean = beanWrapper.getInstance();

        ReflectionEquals comparator = new ReflectionEquals();
        comparator.assertEquals(expectedBean, actualBean);
    }

    @Test
    public void constructFromHibernateLoadWithNulls() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("a", "42");
        props.put("b", "foo");
        props.put("c", "1,3,5,7,11");
        props.put("d", "a--.1,b_.1,c.1");

        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>();
        beanWrapper.setClazz(TestBean.class);
        beanWrapper.setProperties(props);

        TestBean expectedBean = new TestBean(42, "foo", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "a--.1", "b_.1", "c.1" });
        TestBean actualBean = beanWrapper.getInstance();

        ReflectionEquals comparator = new ReflectionEquals();
        comparator.assertEquals(expectedBean, actualBean);
    }

    @Test
    public void testPopulate() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("a", "42");
        props.put("b", "xyzzy");
        props.put("c", "1,3,5,7,11");
        props.put("d", "a,b,c");

        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>(TestBean.class);

        beanWrapper.setProperties(props);

        TestBean expectedBean = new TestBean(42, "xyzzy", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "a", "b", "c" });
        TestBean actualBean = beanWrapper.getInstance();

        ReflectionEquals comparator = new ReflectionEquals();
        comparator.assertEquals(expectedBean, actualBean);
    }

    @Test
    public void testGetProps() throws Exception {
        Map<String, String> expectedProps = new HashMap<>();
        expectedProps.put("a", "42");
        expectedProps.put("b", "xyzzy");
        expectedProps.put("c", "1,3,5,7,11");
        expectedProps.put("d", "a,b,c");

        TestBean bean = new TestBean(42, "xyzzy", new int[] { 1, 3, 5, 7, 11 },
            new String[] { "a", "b", "c" });
        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>(bean);

        assertEquals("BeanWrapper<TestBean>.props", expectedProps,
            BeanWrapper.propertyValueByName(beanWrapper.getTypedProperties()));
    }

    /**
     * Tests that {@link BeanWrapper#getAllPropertyNames()} returns all bean properties, even those
     * that are not set.
     */
    @Test
    public void testAllPropNames() {
        Map<String, String> expectedProps = new HashMap<>();
        expectedProps.put("a", "42");
        expectedProps.put("c", "1,3,5,7,11");

        // Add some properties for bean initialization that have empty and
        // null values. These should be ignored when getting the non-empty
        // properties.
        Map<String, String> allProps = new HashMap<>();
        for (Map.Entry<String, String> entry : expectedProps.entrySet()) {
            allProps.put(entry.getKey(), entry.getValue());
        }

        BeanWrapper<TestBean> beanWrapper = new BeanWrapper<>(TestBean.class);
        beanWrapper.setProperties(allProps);

        Set<String> expectedPropNames = ImmutableSet.of("a", "b", "c", "d");
        assertEquals(expectedPropNames, beanWrapper.getAllPropertyNames());
    }

    /**
     * Tests that {@link BeanWrapper#getAllPropertyNames()} returns properties from derived classes.
     */
    @Test
    public void testGetAllDerivedProps() {
        Map<String, String> providedProps = new HashMap<>();
        providedProps.put("a", "42");
        providedProps.put("b", "xyzzy");
        providedProps.put("c", "1,3,5,7,11");
        providedProps.put("d", "a,b,c");

        BeanWrapper<DerivedBean> beanWrapper = new BeanWrapper<>(DerivedBean.class);
        beanWrapper.setProperties(providedProps);

        Set<String> expectedNames = ImmutableSet.of("a", "b", "c", "d", "e");
        assertEquals(expectedNames, beanWrapper.getAllPropertyNames());
    }

    /**
     * Tests that {@link BeanWrapper#getInstance()} leaves null those bean properties for which
     * empty or all-whitespace string values were provided.
     */
    @Test
    public void testEmptyProps() {
        Map<String, String> props = new HashMap<>();
        props.put("iValue", " ");
        props.put("sValue", " ");
        props.put("iArray", " ");
        props.put("sArray", " ");

        BeanWrapper<NullableBean> beanWrapper = new BeanWrapper<>(NullableBean.class);
        beanWrapper.setProperties(props);

        NullableBean instance = beanWrapper.getInstance();

        assertEquals(0, instance.getiValue());
        assertEquals("", instance.getsValue());
        assertEquals(0, instance.getiArray().length);
        assertEquals(0, instance.getsArray().length);
    }

    /**
     * Tests that instantiating a bean with inherited fields and no supplied properties correctly
     * handles those fields that don't have a zero-argument constructor.
     */
    @Test
    public void testNullPropsWithInheritedFields() {
        Map<String, String> props = new HashMap<>();
        BeanWrapper<DerivedNullableBean> beanWrapper = new BeanWrapper<>(DerivedNullableBean.class);
        beanWrapper.setProperties(props);

        DerivedNullableBean instance = beanWrapper.getInstance();

        assertEquals(0, instance.getiValue());
        assertEquals("", instance.getsValue());
        assertEquals(0, instance.getiArray().length);
        assertEquals(0, instance.getsArray().length);
    }

    /**
     * A bean that inherits from another bean, testing derived class property access.
     */
    public static class DerivedBean extends TestBean {

        private String e;

        public String getE() {
            return e;
        }

        public void setE(String e) {
            this.e = e;
        }

    }

    /**
     * A bean for testing that has fields that can contain nulls.
     */
    public static class NullableBean {

        private int iValue;

        private String sValue;

        private int[] iArray;

        private String[] sArray;

        public int getiValue() {
            return iValue;
        }

        public void setiValue(int iValue) {
            this.iValue = iValue;
        }

        public String getsValue() {
            return sValue;
        }

        public void setsValue(String sValue) {
            this.sValue = sValue;
        }

        public int[] getiArray() {
            return iArray;
        }

        public void setiArray(int[] iArray) {
            this.iArray = iArray;
        }

        public String[] getsArray() {
            return sArray;
        }

        public void setsArray(String[] sArray) {
            this.sArray = sArray;
        }

    }

    /**
     * Implements a derived bean that inherits its fields, so that field access won't work and bean
     * methods must be used.
     */
    public static class DerivedNullableBean extends NullableBean {

        // Nothing added.

    }

}
