package gov.nasa.ziggy.module.hdf5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.parameters.Parameters;

/**
 * Test class for exercising the serialization and deserialization of Persistable objects that
 * contain concrete Parameters classes in various forms.
 *
 * @author PT
 */
public class ParameterSetHdf5Test {

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Test
    public void serializeAndDeserialize() {
        File testDir = directoryRule.directory().toFile();
        ParsTester parsTester = new ParsTester();
        Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();
        File hTest = new File(testDir, "hdf5-pars-test.h5");
        hdf5ModuleInterface.writeFile(hTest, parsTester, true);
        parsTester.setP1(null);
        parsTester.setP2(null);
        parsTester.setP3(null);
        parsTester.setP4(null);

        hdf5ModuleInterface.readFile(hTest, parsTester, true);

        Parameters p1 = parsTester.getP1();
        assertTrue(p1 instanceof ScalarPars);
        assertTrue(((ScalarPars) p1).checkVars());

        Parameters[] p2 = parsTester.getP2();
        assertEquals(2, p2.length);
        for (Parameters p : p2) {
            assertTrue(p instanceof ScalarPars);
            assertTrue(((ScalarPars) p1).checkVars());
        }

        Parameters[] p3 = parsTester.getP3();
        assertEquals(2, p3.length);
        assertTrue(p3[0] instanceof ScalarPars);
        assertTrue(((ScalarPars) p3[0]).checkVars());
        assertTrue(p3[1] instanceof NonScalarPars);
        assertTrue(((NonScalarPars) p3[1]).checkVars());

        List<Parameters> p4 = parsTester.getP4();
        assertEquals(2, p4.size());
        assertTrue(p4.get(0) instanceof NonScalarPars);
        assertTrue(((NonScalarPars) p4.get(0)).checkVars());
        assertTrue(p4.get(1) instanceof ScalarPars);
        assertTrue(((ScalarPars) p4.get(1)).checkVars());
    }

    public static class ScalarPars extends Parameters {

        public int getValue1() {
            return value1;
        }

        public void setValue1(int value1) {
            this.value1 = value1;
        }

        public double getValue2() {
            return value2;
        }

        public void setValue2(double value2) {
            this.value2 = value2;
        }

        public String getValue3() {
            return value3;
        }

        public void setValue3(String value3) {
            this.value3 = value3;
        }

        private int value1 = 100;
        private double value2 = 28.56;
        private String value3 = "surprise!";

        public boolean checkVars() {
            return value1 == 100 && value2 == 28.56 && value3.equals("surprise!");
        }
    }

    public static class NonScalarPars extends Parameters {

        private int[] value1 = { 1, 2, 3 };
        private double[][] value2 = { { 1.1, 2.2 }, { 3.3, 4.4 } };
        private List<String> value3 = Arrays.asList("something", "happening", "here");

        public int[] getValue1() {
            return value1;
        }

        public void setValue1(int[] value1) {
            this.value1 = value1;
        }

        public double[][] getValue2() {
            return value2;
        }

        public void setValue2(double[][] value2) {
            this.value2 = value2;
        }

        public List<String> getValue3() {
            return value3;
        }

        public void setValue3(List<String> value3) {
            this.value3 = value3;
        }

        public boolean checkVars() {
            boolean checkVars = true;
            checkVars = checkVars && Arrays.equals(value1, new int[] { 1, 2, 3 });
            checkVars = checkVars && Arrays.equals(value2[0], new double[] { 1.1, 2.2 });
            checkVars = checkVars && Arrays.equals(value2[1], new double[] { 3.3, 4.4 });
            checkVars = checkVars && value3.size() == 3;
            checkVars = checkVars && value3.get(0).equals("something");
            checkVars = checkVars && value3.get(1).equals("happening");
            return checkVars && value3.get(2).equals("here");
        }
    }

    public static class ParsTester implements Persistable {

        private Parameters p1 = new ScalarPars();
        private Parameters[] p2 = { new ScalarPars(), new ScalarPars() };
        private Parameters[] p3 = { new ScalarPars(), new NonScalarPars() };
        private List<Parameters> p4 = Arrays.asList(new NonScalarPars(), new ScalarPars());

        public Parameters getP1() {
            return p1;
        }

        public void setP1(Parameters p1) {
            this.p1 = p1;
        }

        public Parameters[] getP2() {
            return p2;
        }

        public void setP2(Parameters[] p2) {
            this.p2 = p2;
        }

        public Parameters[] getP3() {
            return p3;
        }

        public void setP3(Parameters[] p3) {
            this.p3 = p3;
        }

        public List<Parameters> getP4() {
            return p4;
        }

        public void setP4(List<Parameters> p4) {
            this.p4 = p4;
        }
    }
}
