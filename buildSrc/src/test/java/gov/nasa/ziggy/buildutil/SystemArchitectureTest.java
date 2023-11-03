package gov.nasa.ziggy.buildutil;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import gov.nasa.ziggy.buildutil.SystemArchitecture.Selector;

/**
 * Unit tests for {@link SystemArchitecture} class.
 *
 * @author PT
 */
public class SystemArchitectureTest {

    @Test
    public void testSelector() {

        Selector<String> selectorSpy = Mockito.spy(SystemArchitecture.selector(String.class));
        Mockito.doReturn(SystemArchitecture.LINUX_INTEL).when(selectorSpy).architecture();
        selectorSpy.linuxIntelObject("A").macIntelObject("B").macM1Object("C");
        assertEquals("A", selectorSpy.get());
        Mockito.doReturn(SystemArchitecture.MAC_INTEL).when(selectorSpy).architecture();
        assertEquals("B", selectorSpy.get());
        Mockito.doReturn(SystemArchitecture.MAC_M1).when(selectorSpy).architecture();
        assertEquals("C", selectorSpy.get());
    }

    @Test
    public void testFastSelectorSyntax() {
        SystemArchitecture.selector(String.class)
            .linuxIntelObject("A")
            .macIntelObject("B")
            .macM1Object("C")
            .get();
    }
}
