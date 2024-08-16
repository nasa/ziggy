package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ClassWrapperTest {

    @Test
    public void testSimpleClassConstructor() {
        ClassWrapper<Foo> fooWrapper = new ClassWrapper<>(Foo.class);
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Foo",
            fooWrapper.getClassName());
        assertEquals(Foo.class, fooWrapper.getClazz());
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Foo",
            fooWrapper.unmangledClassName());
        assertEquals(Foo.class, fooWrapper.newInstance().getClass());
    }

    @Test
    public void testExtendedClassConstructor() {
        ClassWrapper<Foo> fooWrapper = new ClassWrapper<>(Bar.class);
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Bar",
            fooWrapper.getClassName());
        assertEquals(Bar.class, fooWrapper.getClazz());
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Bar",
            fooWrapper.unmangledClassName());
        assertEquals(Bar.class, fooWrapper.newInstance().getClass());
    }

    @Test
    public void testInstanceConstructor() {
        ClassWrapper<Foo> fooWrapper = new ClassWrapper<>(new Foo());
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Foo",
            fooWrapper.getClassName());
        assertEquals(Foo.class, fooWrapper.getClazz());
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Foo",
            fooWrapper.unmangledClassName());
        assertEquals(Foo.class, fooWrapper.newInstance().getClass());
    }

    @Test
    public void testExtendedInstanceConstructor() {
        ClassWrapper<Foo> fooWrapper = new ClassWrapper<>(new Bar());
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Bar",
            fooWrapper.getClassName());
        assertEquals(Bar.class, fooWrapper.getClazz());
        assertEquals("gov.nasa.ziggy.pipeline.definition.ClassWrapperTest$Bar",
            fooWrapper.unmangledClassName());
        assertEquals(Bar.class, fooWrapper.newInstance().getClass());
    }

    public static class Foo {
    }

    public static class Bar extends Foo {
    }
}
