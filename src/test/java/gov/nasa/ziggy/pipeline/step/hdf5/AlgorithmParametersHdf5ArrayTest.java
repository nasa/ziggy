package gov.nasa.ziggy.pipeline.step.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.step.AlgorithmParameters;
import gov.nasa.ziggy.util.ZiggyCollectionUtils;
import gov.nasa.ziggy.util.io.Persistable;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5O_token_t;

/**
 * Unit test class for {@link AlgorithmParametersHdf5Array} class.
 *
 * @author PT
 */
public class AlgorithmParametersHdf5ArrayTest {

    private AlgorithmInterfaceTester testArticle;
    private File workingDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {

        workingDir = directoryRule.directory().toFile();
        workingDir.mkdirs();
        testArticle = new AlgorithmInterfaceTester();
        testArticle.setAlgorithmParameters(populateAlgorithmParameters());
    }

    @Test
    public void testArrayFactory() throws NoSuchFieldException, SecurityException {

        AbstractHdf5Array h1 = AbstractHdf5Array.newInstance(testArticle.getPrimitiveValue());
        assertTrue(h1 instanceof PrimitiveHdf5Array);
        AbstractHdf5Array h2 = AbstractHdf5Array.newInstance(testArticle.getPersistableValue());
        assertTrue(h2 instanceof PersistableHdf5Array);
        AbstractHdf5Array h3 = AbstractHdf5Array.newInstance(testArticle.getAlgorithmParameters());
        assertTrue(h3 instanceof AlgorithmParametersHdf5Array);

        Class<? extends AlgorithmInterfaceTester> clazz = testArticle.getClass();
        h1 = AbstractHdf5Array.newInstance(clazz.getDeclaredField("primitiveValue"));
        assertTrue(h1 instanceof PrimitiveHdf5Array);
        h2 = AbstractHdf5Array.newInstance(clazz.getDeclaredField("persistableValue"));
        assertTrue(h2 instanceof PersistableHdf5Array);
        h3 = AbstractHdf5Array.newInstance(clazz.getDeclaredField("algorithmParameters"));
        assertTrue(h3 instanceof AlgorithmParametersHdf5Array);
    }

    @Test
    public void testWriteAndRead() {
        File testFile = new File(workingDir, "test-file.h5");
        Hdf5AlgorithmInterface hdf5AlgorithmInterface = new Hdf5AlgorithmInterface();
        hdf5AlgorithmInterface.writeFile(testFile, testArticle, true);

        AlgorithmInterfaceTester loadArticle = new AlgorithmInterfaceTester();
        hdf5AlgorithmInterface.readFile(testFile, loadArticle, true);
        AlgorithmParameters m = loadArticle.getAlgorithmParameters();
        Map<String, ParameterSet> p = m.getParameterSetsByName();
        assertEquals(1, p.size());
        ParameterSet d = p.get("test default parameters");
        assertNotNull(d);
        assertEquals(d.getName(), "test default parameters");
        assertEquals(d.getParameterSetNameOrAlgorithmInterfaceName(), "algorithmInterfaceName");
        assertEquals(d.getAlgorithmInterfaceName(), "algorithmInterfaceName");
        assertTrue(d.hasAlgorithmInterfaceName());
        Set<Parameter> t = d.getParameters();
        assertEquals(4, t.size());
        Map<String, Parameter> typedPropertyMap = new HashMap<>();
        for (Parameter property : t) {
            typedPropertyMap.put(property.getName(), property);
        }
        Parameter typedProp = typedPropertyMap.get("intValue");
        assertEquals("300", typedProp.getString());
        assertEquals(ZiggyDataType.ZIGGY_INT, typedProp.getDataType());
        assertTrue(typedProp.isScalar());
        typedProp = typedPropertyMap.get("doubleArray");
        assertEquals("105.3,92.7", typedProp.getString());
        assertEquals(ZiggyDataType.ZIGGY_DOUBLE, typedProp.getDataType());
        assertFalse(typedProp.isScalar());
        typedProp = typedPropertyMap.get("falseValue");
        assertEquals("false", typedProp.getString());
        assertEquals(ZiggyDataType.ZIGGY_BOOLEAN, typedProp.getDataType());
        assertTrue(typedProp.isScalar());
        typedProp = typedPropertyMap.get("trueValue");
        assertEquals("true", typedProp.getString());
        assertEquals(ZiggyDataType.ZIGGY_BOOLEAN, typedProp.getDataType());
        assertTrue(typedProp.isScalar());

        // We also want to see that the parameter set was stored with
        // its algorithm interface name rather than the parameter set name.
        // So -- direct, low-level access to the HDF5 file, tedious as
        // it may seem, is our only option.
        long fileId = H5.H5Fopen(testFile.getAbsolutePath(), HDF5Constants.H5F_ACC_RDONLY,
            H5P_DEFAULT);
        int count = (int) H5.H5Gn_members(fileId, "/algorithmParameters");
        String[] oname = new String[count];
        int[] otype = new int[count];
        int[] ltype = new int[count];
        H5O_token_t[] otokens = new H5O_token_t[count];
        H5.H5Gget_obj_info_all(fileId, "/algorithmParameters", oname, otype, ltype, otokens,
            HDF5Constants.H5_INDEX_NAME);
        H5.H5Fclose(fileId);
        assertEquals("algorithmInterfaceName", oname[0]);
    }

    @Test
    public void testWriteAndReadNoAlgorithmInterfaceName() {

        File testFile = new File(workingDir, "test-file.h5");
        ParameterSet parameterSet = testArticle.getAlgorithmParameters()
            .getParameterSetsByName()
            .get("test default parameters");
        parameterSet.setAlgorithmInterfaceName(null);
        Hdf5AlgorithmInterface hdf5AlgorithmInterface = new Hdf5AlgorithmInterface();
        hdf5AlgorithmInterface.writeFile(testFile, testArticle, true);

        AlgorithmInterfaceTester loadArticle = new AlgorithmInterfaceTester();
        hdf5AlgorithmInterface.readFile(testFile, loadArticle, true);
        AlgorithmParameters m = loadArticle.getAlgorithmParameters();
        Map<String, ParameterSet> p = m.getParameterSetsByName();
        ParameterSet d = p.get("test default parameters");
        assertNotNull(d);
        assertEquals(d.getName(), "test default parameters");
        assertEquals(d.getParameterSetNameOrAlgorithmInterfaceName(), "test default parameters");
        assertNull(d.getAlgorithmInterfaceName());
        assertFalse(d.hasAlgorithmInterfaceName());
    }

    private AlgorithmParameters populateAlgorithmParameters() {
        AlgorithmParameters m = new AlgorithmParameters();
        ParameterSet parameterSet = parameterSet("test default parameters",
            "algorithmInterfaceName");
        m.getParameterSetsByName().put(parameterSet.getName(), parameterSet);

        return m;
    }

    @Test
    public void testFieldOrder() {
        testArticle.getAlgorithmParameters()
            .addParameterSets(ZiggyCollectionUtils
                .mutableSetOf(parameterSet("second parameter set", "secondParameterSet")));
        File testFile = new File(workingDir, "test-file.h5");
        Hdf5AlgorithmInterface hdf5AlgorithmInterface = new Hdf5AlgorithmInterface();
        hdf5AlgorithmInterface.writeFile(testFile, testArticle, true);

        Set<Integer> orderAttributeValues = new HashSet<>();
        long fileId = H5.H5Fopen(testFile.toString(), HDF5Constants.H5F_ACC_RDONLY, H5P_DEFAULT);
        long algorithmParametersGroup = openGroupIfPresent(fileId, "algorithmParameters");
        long parameterSetGroup = openGroupIfPresent(algorithmParametersGroup,
            "algorithmInterfaceName");
        long attributeId = H5.H5Aopen(parameterSetGroup,
            Hdf5AlgorithmInterface.FIELD_ORDER_ATT_NAME, H5P_DEFAULT);
        int[] orderArray = new int[1];
        H5.H5Aread(attributeId, ZIGGY_LONG.getHdf5Type(), orderArray);
        orderAttributeValues.add(orderArray[0]);
        H5.H5Aclose(attributeId);
        H5.H5Gclose(parameterSetGroup);

        parameterSetGroup = openGroupIfPresent(algorithmParametersGroup, "secondParameterSet");
        attributeId = H5.H5Aopen(parameterSetGroup, Hdf5AlgorithmInterface.FIELD_ORDER_ATT_NAME,
            H5P_DEFAULT);
        H5.H5Aread(attributeId, ZIGGY_LONG.getHdf5Type(), orderArray);
        orderAttributeValues.add(orderArray[0]);
        H5.H5Aclose(attributeId);
        H5.H5Gclose(parameterSetGroup);
        H5.H5Gclose(algorithmParametersGroup);
        H5.H5Fclose(fileId);

        assertTrue(orderAttributeValues.contains(0));
        assertTrue(orderAttributeValues.contains(1));
    }

    private long openGroupIfPresent(long fileId, String groupName) {
        long groupId = -1;
        if (H5.H5Lexists(fileId, groupName, H5P_DEFAULT)) {
            groupId = H5.H5Gopen(fileId, groupName, H5P_DEFAULT);
        }
        return groupId;
    }

    private ParameterSet parameterSet(String name, String algorithmInterfaceName) {
        ParameterSet d = new ParameterSet();
        d.setName(name);
        d.setAlgorithmInterfaceName(algorithmInterfaceName);
        Set<Parameter> typedProperties = new HashSet<>();
        typedProperties.add(new Parameter("intValue", "300", ZiggyDataType.ZIGGY_INT));
        typedProperties
            .add(new Parameter("doubleArray", "105.3, 92.7", ZiggyDataType.ZIGGY_DOUBLE, false));
        typedProperties.add(new Parameter("trueValue", "true", ZiggyDataType.ZIGGY_BOOLEAN));
        typedProperties.add(new Parameter("falseValue", "false", ZiggyDataType.ZIGGY_BOOLEAN));
        d.setParameters(typedProperties);
        return d;
    }

    private static class AlgorithmInterfaceTester implements Persistable {

        private int primitiveValue = 1;
        private PersistableSample1 persistableValue = PersistableSample1.newInstance(1, 1, 1, 1, 1,
            1, 1);
        private AlgorithmParameters algorithmParameters = new AlgorithmParameters();

        public int getPrimitiveValue() {
            return primitiveValue;
        }

        public PersistableSample1 getPersistableValue() {
            return persistableValue;
        }

        public AlgorithmParameters getAlgorithmParameters() {
            return algorithmParameters;
        }

        public void setAlgorithmParameters(AlgorithmParameters algorithmParameters) {
            this.algorithmParameters = algorithmParameters;
        }
    }
}
