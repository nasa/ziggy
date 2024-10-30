package gov.nasa.ziggy.uow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import gov.nasa.ziggy.pipeline.definition.Parameter;

public class UnitOfWorkTest {

    private static final Collection<Parameter> PARAMETERS = Set.of(
        new Parameter(UnitOfWork.BRIEF_STATE_PARAMETER_NAME, "brief1"),
        new Parameter("C", "valueC"), new Parameter("B", "valueB"), new Parameter("A", "valueA"));

    @Test
    public void testBriefState() {
        assertEquals("brief1", createUnitOfWork().briefState());
    }

    @Test
    public void testSetBriefState() {
        try {
            UnitOfWork unitOfWork = new UnitOfWork();
            unitOfWork.setBriefState(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException expected) {
        }
        try {
            UnitOfWork unitOfWork = new UnitOfWork();
            unitOfWork.setBriefState(" ");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        UnitOfWork unitOfWork = createUnitOfWork();
        unitOfWork.setBriefState("brief2");
        assertEquals("brief2", unitOfWork.briefState());
    }

    @Test
    public void testAddParameter() {
        try {
            UnitOfWork unitOfWork = new UnitOfWork();
            unitOfWork.addParameter(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException expected) {
        }

        UnitOfWork unitOfWork = createUnitOfWork();
        Parameter parameter = new Parameter("new parameter", "parameter value");
        unitOfWork.addParameter(parameter);
        verifyParameter(parameter, unitOfWork.getParameter(parameter.getName()));
    }

    @Test
    public void testGetParameter() {
        UnitOfWork unitOfWork = createUnitOfWork();
        assertEquals(null, unitOfWork.getParameter(null));
        for (Parameter parameter : PARAMETERS) {
            verifyParameter(parameter, unitOfWork.getParameter(parameter.getName()));
        }
    }

    @Test
    public void testGetParameters() {
        UnitOfWork unitOfWork = new UnitOfWork();
        assertEquals(0, unitOfWork.getParameters().size());

        unitOfWork = createUnitOfWork();
        assertEquals(4, unitOfWork.getParameters().size());
        assertEquals(PARAMETERS, unitOfWork.getParameters());
    }

    @Test
    public void testSetParameters() {
        try {
            UnitOfWork unitOfWork = new UnitOfWork();
            unitOfWork.setParameters(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException expected) {
        }
        try {
            UnitOfWork unitOfWork = new UnitOfWork();
            unitOfWork.setParameters(new HashSet<>());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        Collection<Parameter> newParameters = Set.of(
            new Parameter(UnitOfWork.BRIEF_STATE_PARAMETER_NAME, "brief2"),
            new Parameter("CC", "valueCC"), new Parameter("BB", "valueBB"),
            new Parameter("AA", "valueAA"));
        UnitOfWork unitOfWork = createUnitOfWork();
        unitOfWork.setParameters(newParameters);
        assertNotEquals(PARAMETERS, unitOfWork.getParameters());
        assertEquals(newParameters, unitOfWork.getParameters());
    }

    @Test
    public void testCompareTo() {
        UnitOfWork unitOfWork = createUnitOfWork();
        List<Parameter> parameters = new ArrayList<>(new TreeSet<>(unitOfWork.getParameters()));
        assertEquals(4, parameters.size());
        assertEquals("valueA", parameters.get(0).getValue());
        assertEquals("valueB", parameters.get(1).getValue());
        assertEquals("valueC", parameters.get(2).getValue());
        assertEquals("brief1", parameters.get(3).getValue());
    }

    private UnitOfWork createUnitOfWork() {
        UnitOfWork unitOfWork = new UnitOfWork();
        unitOfWork.setParameters(PARAMETERS);
        return unitOfWork;
    }

    private void verifyParameter(Parameter expectedParameter, Parameter actualParameter) {
        assertEquals(expectedParameter, actualParameter);
        assertEquals(expectedParameter.getName(), actualParameter.getName());
        assertEquals(expectedParameter.getValue(), actualParameter.getValue());
    }
}
