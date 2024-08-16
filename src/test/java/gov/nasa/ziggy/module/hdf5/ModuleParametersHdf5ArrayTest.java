package gov.nasa.ziggy.module.hdf5;

import static gov.nasa.ziggy.collections.ZiggyDataType.ZIGGY_LONG;
import static hdf.hdf5lib.HDF5Constants.H5P_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
import gov.nasa.ziggy.module.ModuleParameters;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.util.ZiggyCollectionUtils;
import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.structs.H5O_token_t;

/**
 * Unit test class for {@link ModuleParametersHdf5Array} class.
 *
 * @author PT
 */
public class ModuleParametersHdf5ArrayTest {

    private ModInterfaceTester testArticle;
    private File workingDir;

    @Rule
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Before
    public void setup() {

        workingDir = directoryRule.directory().toFile();
        workingDir.mkdirs();
        testArticle = new ModInterfaceTester();
        testArticle.setModuleParameters(populateModuleParameters());
    }

    @Test
    public void testArrayFactory() throws NoSuchFieldException, SecurityException {

        AbstractHdf5Array h1 = AbstractHdf5Array.newInstance(testArticle.getPrimitiveValue());
        assertTrue(h1 instanceof PrimitiveHdf5Array);
        AbstractHdf5Array h2 = AbstractHdf5Array.newInstance(testArticle.getPersistableValue());
        assertTrue(h2 instanceof PersistableHdf5Array);
        AbstractHdf5Array h3 = AbstractHdf5Array.newInstance(testArticle.getModuleParameters());
        assertTrue(h3 instanceof ModuleParametersHdf5Array);

        Class<? extends ModInterfaceTester> clazz = testArticle.getClass();
        h1 = AbstractHdf5Array.newInstance(clazz.getDeclaredField("primitiveValue"));
        assertTrue(h1 instanceof PrimitiveHdf5Array);
        h2 = AbstractHdf5Array.newInstance(clazz.getDeclaredField("persistableValue"));
        assertTrue(h2 instanceof PersistableHdf5Array);
        h3 = AbstractHdf5Array.newInstance(clazz.getDeclaredField("moduleParameters"));
        assertTrue(h3 instanceof ModuleParametersHdf5Array);
    }

    @Test
    public void testWriteAndRead() {
        File testFile = new File(workingDir, "test-file.h5");
        Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();
        hdf5ModuleInterface.writeFile(testFile, testArticle, true);

        ModInterfaceTester loadArticle = new ModInterfaceTester();
        loadArticle.setPersistableValue(null);
        loadArticle.setPrimitiveValue(0);
        loadArticle.setModuleParameters(new ModuleParameters());
        hdf5ModuleInterface.readFile(testFile, loadArticle, true);
        ModuleParameters m = loadArticle.getModuleParameters();
        Map<String, ParameterSet> p = m.getParameterSetsByName();
        assertEquals(1, p.size());
        ParameterSet d = p.get("test default parameters");
        assertNotNull(d);
        assertEquals(d.getName(), "test default parameters");
        assertEquals(d.getModuleInterfaceName(), "moduleInterfaceName");
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
        // its module interface name rather than the parameter set name.
        // So -- direct, low-level access to the HDF5 file, tedious as
        // it may seem, is our only option.
        long fileId = H5.H5Fopen(testFile.getAbsolutePath(), HDF5Constants.H5F_ACC_RDONLY,
            H5P_DEFAULT);
        int count = (int) H5.H5Gn_members(fileId, "/moduleParameters");
        String[] oname = new String[count];
        int[] otype = new int[count];
        int[] ltype = new int[count];
        H5O_token_t[] otokens = new H5O_token_t[count];
        H5.H5Gget_obj_info_all(fileId, "/moduleParameters", oname, otype, ltype, otokens,
            HDF5Constants.H5_INDEX_NAME);
        H5.H5Fclose(fileId);
        assertEquals("moduleInterfaceName", oname[0]);
    }

    private ModuleParameters populateModuleParameters() {
        ModuleParameters m = new ModuleParameters();
        ParameterSet parameterSet = parameterSet("test default parameters", "moduleInterfaceName");
        m.getParameterSetsByName().put(parameterSet.getName(), parameterSet);

        return m;
    }

    @Test
    public void testFieldOrder() {
        testArticle.getModuleParameters()
            .addParameterSets(ZiggyCollectionUtils
                .mutableSetOf(parameterSet("second parameter set", "secondParameterSet")));
        File testFile = new File(workingDir, "test-file.h5");
        Hdf5ModuleInterface hdf5ModuleInterface = new Hdf5ModuleInterface();
        hdf5ModuleInterface.writeFile(testFile, testArticle, true);

        Set<Integer> orderAttributeValues = new HashSet<>();
        // Now we have to use low-level functions to access the content of the HDF5 file.
        long fileId;
        fileId = H5.H5Fopen(testFile.toString(), HDF5Constants.H5F_ACC_RDONLY, H5P_DEFAULT);
        long moduleParametersGroup = openGroupIfPresent(fileId, "moduleParameters");
        long parameterSetGroup = openGroupIfPresent(moduleParametersGroup, "moduleInterfaceName");
        long attributeId = H5.H5Aopen(parameterSetGroup, Hdf5ModuleInterface.FIELD_ORDER_ATT_NAME,
            H5P_DEFAULT);
        int[] orderArray = new int[1];
        H5.H5Aread(attributeId, ZIGGY_LONG.getHdf5Type(), orderArray);
        orderAttributeValues.add(orderArray[0]);
        H5.H5Aclose(attributeId);
        H5.H5Gclose(parameterSetGroup);

        parameterSetGroup = openGroupIfPresent(moduleParametersGroup, "secondParameterSet");
        attributeId = H5.H5Aopen(parameterSetGroup, Hdf5ModuleInterface.FIELD_ORDER_ATT_NAME,
            H5P_DEFAULT);
        H5.H5Aread(attributeId, ZIGGY_LONG.getHdf5Type(), orderArray);
        orderAttributeValues.add(orderArray[0]);
        H5.H5Aclose(attributeId);
        H5.H5Gclose(parameterSetGroup);
        H5.H5Gclose(moduleParametersGroup);
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

    private ParameterSet parameterSet(String name, String moduleInterfaceName) {
        ParameterSet d = new ParameterSet();
        d.setName(name);
        d.setModuleInterfaceName(moduleInterfaceName);
        Set<Parameter> typedProperties = new HashSet<>();
        typedProperties.add(new Parameter("intValue", "300", ZiggyDataType.ZIGGY_INT));
        typedProperties
            .add(new Parameter("doubleArray", "105.3, 92.7", ZiggyDataType.ZIGGY_DOUBLE, false));
        typedProperties.add(new Parameter("trueValue", "true", ZiggyDataType.ZIGGY_BOOLEAN));
        typedProperties.add(new Parameter("falseValue", "false", ZiggyDataType.ZIGGY_BOOLEAN));
        d.setParameters(typedProperties);
        return d;
    }

    private static class ModInterfaceTester implements Persistable {

        private int primitiveValue = 1;
        private PersistableSample1 persistableValue = PersistableSample1.newInstance(1, 1, 1, 1, 1,
            1, 1);
        private ModuleParameters moduleParameters = new ModuleParameters();

        public int getPrimitiveValue() {
            return primitiveValue;
        }

        public void setPrimitiveValue(int primitiveValue) {
            this.primitiveValue = primitiveValue;
        }

        public PersistableSample1 getPersistableValue() {
            return persistableValue;
        }

        public void setPersistableValue(PersistableSample1 persistableValue) {
            this.persistableValue = persistableValue;
        }

        public ModuleParameters getModuleParameters() {
            return moduleParameters;
        }

        public void setModuleParameters(ModuleParameters moduleParameters) {
            this.moduleParameters = moduleParameters;
        }
    }
}
