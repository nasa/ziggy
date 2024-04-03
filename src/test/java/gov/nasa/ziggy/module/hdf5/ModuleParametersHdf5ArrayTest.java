package gov.nasa.ziggy.module.hdf5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.module.io.Persistable;
import gov.nasa.ziggy.parameters.ModuleParameters;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.pipeline.definition.TypedParameter;

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
        List<ParametersInterface> p = m.getModuleParameters();
        assertEquals(1, p.size());
        Parameters d;
        d = (Parameters) p.get(0);
        assertEquals(d.getName(), "test default parameters");
        Set<TypedParameter> t = d.getParameters();
        assertEquals(4, t.size());
        Map<String, TypedParameter> typedPropertyMap = new HashMap<>();
        for (TypedParameter property : t) {
            typedPropertyMap.put(property.getName(), property);
        }
        TypedParameter typedProp = typedPropertyMap.get("intValue");
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
    }

    private ModuleParameters populateModuleParameters() {
        ModuleParameters m = new ModuleParameters();
        List<ParametersInterface> p = m.getModuleParameters();
        p.add(parameters());

        return m;
    }

    private Parameters parameters() {
        Parameters d = new Parameters();
        d.setName("test default parameters");
        Set<TypedParameter> typedProperties = new HashSet<>();
        typedProperties.add(new TypedParameter("intValue", "300", ZiggyDataType.ZIGGY_INT));
        typedProperties.add(
            new TypedParameter("doubleArray", "105.3, 92.7", ZiggyDataType.ZIGGY_DOUBLE, false));
        typedProperties.add(new TypedParameter("trueValue", "true", ZiggyDataType.ZIGGY_BOOLEAN));
        typedProperties.add(new TypedParameter("falseValue", "false", ZiggyDataType.ZIGGY_BOOLEAN));
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
