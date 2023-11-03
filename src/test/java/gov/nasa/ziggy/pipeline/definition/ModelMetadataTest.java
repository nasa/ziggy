package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the ModelMetadata class.
 *
 * @author PT
 */
public class ModelMetadataTest {

    private ModelType modelType1, modelType2, modelType3;
    private static Date fixedDate = new GregorianCalendar(2020, 11, 29, 11, 15, 16).getTime();

    @Before
    public void setup() {

        // Set up the model type 1 to have a model ID in its name, which is a simple integer,
        // and a timestamp in its name
        modelType1 = new ModelType();
        modelType1.setFileNameRegex("tess([0-9]{13})-([0-9]{5})_([0-9]{3})-geometry.xml");
        modelType1.setType("geometry");
        modelType1.setVersionNumberGroup(3);
        modelType1.setTimestampGroup(1);
        modelType1.setSemanticVersionNumber(false);

        // Set up the model type 2 to have a semantic model ID in its name but no timestamp
        modelType2 = new ModelType();
        modelType2.setFileNameRegex("calibration-([0-9]+\\.[0-9]+\\.[0-9]+).h5");
        modelType2.setTimestampGroup(-1);
        modelType2.setType("calibration");
        modelType2.setVersionNumberGroup(1);
        modelType2.setSemanticVersionNumber(true);

        // Set up the model type 3 to have neither ID nor timestamp
        modelType3 = new ModelType();
        modelType3.setFileNameRegex("simple-text.h5");
        modelType3.setType("ravenswood");
        modelType3.setTimestampGroup(-1);
        modelType3.setVersionNumberGroup(-1);
    }

    @Test
    public void testConstructorNoExistingVersion() {

        String filename1 = "tess2020321141516-12345_022-geometry.xml";
        ModelMetadata modelMetadata = new ModelMetadataFixedDate(modelType1, filename1, "desc",
            null);
        assertEquals("022", modelMetadata.getModelRevision());
        assertEquals(filename1, modelMetadata.getDatastoreFileName());
        assertEquals(filename1, modelMetadata.getOriginalFileName());
        assertEquals("desc", modelMetadata.getModelDescription());
        assertNull(modelMetadata.getLockTime());
        assertEquals(modelType1, modelMetadata.getModelType());
        assertFalse(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getImportTime());

        String filename2 = "calibration-4.12.9.h5";
        modelMetadata = new ModelMetadataFixedDate(modelType2, filename2, "blabla", null);
        assertEquals("4.12.9", modelMetadata.getModelRevision());
        assertEquals("2020-12-29." + filename2, modelMetadata.getDatastoreFileName());
        assertEquals(filename2, modelMetadata.getOriginalFileName());
        assertEquals("blabla", modelMetadata.getModelDescription());
        assertNull(modelMetadata.getLockTime());
        assertEquals(modelType2, modelMetadata.getModelType());
        assertFalse(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getImportTime());

        String filename3 = "simple-text.h5";
        modelMetadata = new ModelMetadataFixedDate(modelType3, filename3, "zinfandel", null);
        assertEquals("1", modelMetadata.getModelRevision());
        assertEquals("2020-12-29.0001-" + filename3, modelMetadata.getDatastoreFileName());
        assertEquals(filename3, modelMetadata.getOriginalFileName());
        assertEquals("zinfandel", modelMetadata.getModelDescription());
        assertNull(modelMetadata.getLockTime());
        assertEquals(modelType3, modelMetadata.getModelType());
        assertFalse(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getImportTime());
    }

    @Test
    public void testConstructorExistingVersion() {

        // For the modelType1 case, nothing different should happen
        ModelMetadata existingMetadata = new ModelMetadata();
        existingMetadata.setModelType(modelType1);
        existingMetadata.setModelRevision("020");
        String filename1 = "tess2020321141516-12345_022-geometry.xml";
        ModelMetadata modelMetadata = new ModelMetadataFixedDate(modelType1, filename1, "desc",
            existingMetadata);
        assertEquals("022", modelMetadata.getModelRevision());
        assertEquals(filename1, modelMetadata.getDatastoreFileName());
        assertEquals(filename1, modelMetadata.getOriginalFileName());
        assertEquals("desc", modelMetadata.getModelDescription());
        assertNull(modelMetadata.getLockTime());
        assertEquals(modelType1, modelMetadata.getModelType());
        assertFalse(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getImportTime());

        // For the modelType2 case, nothing different should happen
        existingMetadata.setModelRevision("4.11.0");
        existingMetadata.setModelType(modelType2);
        String filename2 = "calibration-4.12.9.h5";
        modelMetadata = new ModelMetadataFixedDate(modelType2, filename2, "blabla",
            existingMetadata);
        assertEquals("4.12.9", modelMetadata.getModelRevision());
        assertEquals("2020-12-29." + filename2, modelMetadata.getDatastoreFileName());
        assertEquals(filename2, modelMetadata.getOriginalFileName());
        assertEquals("blabla", modelMetadata.getModelDescription());
        assertNull(modelMetadata.getLockTime());
        assertEquals(modelType2, modelMetadata.getModelType());
        assertFalse(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getImportTime());

        // for modelType3, the new version number should increment the existing one
        existingMetadata.setModelRevision("20");
        existingMetadata.setModelType(modelType3);
        String filename3 = "simple-text.h5";
        modelMetadata = new ModelMetadataFixedDate(modelType3, filename3, "zinfandel",
            existingMetadata);
        assertEquals("21", modelMetadata.getModelRevision());
        assertEquals("2020-12-29.0021-" + filename3, modelMetadata.getDatastoreFileName());
        assertEquals(filename3, modelMetadata.getOriginalFileName());
        assertEquals("zinfandel", modelMetadata.getModelDescription());
        assertNull(modelMetadata.getLockTime());
        assertEquals(modelType3, modelMetadata.getModelType());
        assertFalse(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getImportTime());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionFromVersionNumber1() {
        ModelMetadata existingMetadata = new ModelMetadata();
        existingMetadata.setModelType(modelType1);
        existingMetadata.setModelRevision("023");
        String filename1 = "tess2020321141516-12345_022-geometry.xml";
        new ModelMetadataFixedDate(modelType1, filename1, "desc", existingMetadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionFromVersionNumber2() {
        ModelMetadata existingMetadata = new ModelMetadata();
        existingMetadata.setModelRevision("4.12.10");
        existingMetadata.setModelType(modelType2);
        String filename2 = "calibration-4.12.9.h5";
        new ModelMetadataFixedDate(modelType2, filename2, "blabla", existingMetadata);
    }

    // NOTE: the modelType3 can't produce this IllegalArgumentException because the
    // model file doesn't have a version number, thus the version number of the new
    // model file can't be below the one currently in the registry.

    public void testLock() {
        String filename1 = "tess2020321141516-12345_022-geometry.xml";
        ModelMetadata modelMetadata = new ModelMetadataFixedDate(modelType1, filename1, "desc",
            null);
        modelMetadata.lock();
        assertTrue(modelMetadata.isLocked());
        assertEquals(fixedDate, modelMetadata.getLockTime());
    }

    /**
     * This class overrides the currentDate() method of ModelMetadata but is otherwise identical.
     * This allows us to properly test the methods that rely on obtaining a Date instance.
     *
     * @author PT
     */
    public static class ModelMetadataFixedDate extends ModelMetadata {

        public ModelMetadataFixedDate(ModelType modelType, String modelName,
            String modelDescription, ModelMetadata currentRegistryMetadata) {
            super(modelType, modelName, modelDescription, currentRegistryMetadata);
        }

        @Override
        Date currentDate() {
            return fixedDate;
        }

        public ModelMetadata toSuper() {
            return new ModelMetadata(this);
        }
    }
}
