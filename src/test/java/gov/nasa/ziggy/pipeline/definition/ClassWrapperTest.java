package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;

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

    @Test
    public void testParameterSetConstructor() {
        ParameterSet paramSet = new ParameterSet();
        Parameters params = new Parameters();
        paramSet.populateFromParametersInstance(params);
        paramSet.setName("foo");
        ClassWrapper<ParametersInterface> paramWrapper = new ClassWrapper<>(paramSet);
        assertEquals("gov.nasa.ziggy.parameters.Parameters foo", paramWrapper.getClassName());
        assertEquals("gov.nasa.ziggy.parameters.Parameters", paramWrapper.unmangledClassName());
        assertEquals(Parameters.class, paramWrapper.getClazz());
    }

    @Test
    public void testParameterSetSubclassConstructor() {
        ParameterSet paramSet = new ParameterSet();
        RemoteParameters params = new RemoteParameters();
        paramSet.populateFromParametersInstance(params);
        paramSet.setName("foo");
        ClassWrapper<ParametersInterface> paramWrapper = new ClassWrapper<>(paramSet);
        assertEquals("gov.nasa.ziggy.module.remote.RemoteParameters", paramWrapper.getClassName());
        assertEquals("gov.nasa.ziggy.module.remote.RemoteParameters",
            paramWrapper.unmangledClassName());
        assertEquals(RemoteParameters.class, paramWrapper.getClazz());
    }

    public static class Foo {
    }

    public static class Bar extends Foo {
    }
}
