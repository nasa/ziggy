package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit test class for ModelType.
 *
 * @author PT
 */
public class ModelTypeTest {

    private ModelType modelType;

    @Before
    public void setup() {

        modelType = new ModelType();
        modelType.setType("geometry");
        modelType.setFileNameRegex("tess([0-9]{13})-([0-9]{5})_([0-9]{3})-geometry.xml");
        modelType.setTimestampGroup(1);
        modelType.setVersionNumberGroup(3);
        modelType.setSemanticVersionNumber(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testValidationFailsForVersionNumberGroup() {
        modelType.setVersionNumberGroup(10);
        modelType.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testValidationFailsForTimestampGroup() {
        modelType.setTimestampGroup(10);
        modelType.validate();
    }

    @Test(expected = IllegalStateException.class)
    public void testValidationFailsSemanticVersionNumber() {
        modelType.setVersionNumberGroup(-1);
        modelType.setSemanticVersionNumber(true);
        modelType.validate();
    }

    @Test
    public void testGetPattern() {
        Pattern pattern = modelType.pattern();
        assertEquals("tess([0-9]{13})-([0-9]{5})_([0-9]{3})-geometry.xml", pattern.pattern());
    }

    @Test
    public void testIsThisModelType() {
        String s1 = "tess2021028132020-23105_122-geometry.xml";
        assertTrue(modelType.isThisModelType(s1));
        String s2 = "tess2021028132020-23105_122-readnoise.xml";
        assertFalse(modelType.isThisModelType(s2));
    }

    @Test
    public void testVersionNumber() {
        String s1 = "tess2021028132020-23105_122-geometry.xml";
        assertEquals("122", modelType.versionNumber(s1));
        String s2 = "tess2021028132020-23105_122-readnoise.xml";
        assertTrue(modelType.versionNumber(s2).isEmpty());
        modelType.setVersionNumberGroup(-1);
        modelType.validate();
        assertTrue(modelType.versionNumber(s1).isEmpty());
    }
}
