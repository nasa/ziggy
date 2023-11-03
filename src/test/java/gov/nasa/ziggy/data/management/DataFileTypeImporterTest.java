package gov.nasa.ziggy.data.management;

import static gov.nasa.ziggy.ZiggyUnitTestUtils.TEST_DATA;
import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;

import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.crud.DataFileTypeCrud;
import gov.nasa.ziggy.pipeline.definition.crud.ModelCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import jakarta.xml.bind.JAXBException;

/**
 * Unit test class for DataFileTypeImporter.
 *
 * @author PT
 */
public class DataFileTypeImporterTest {

    private static final Path DATASTORE = TEST_DATA.resolve("datastore");
    private static final String FILE_1 = DATASTORE.resolve("pd-test-1.xml").toString();

    private static final String FILE_2 = DATASTORE.resolve("pd-test-2.xml").toString();
    private static final String NO_SUCH_FILE = "no-such-file.xml";
    private static final String NOT_REGULAR_FILE = TEST_DATA.resolve("configuration").toString();
    private static final String INVALID_FILE_1 = DATASTORE.resolve("pd-test-invalid-type.xml")
        .toString();
    private static final String INVALID_FILE_2 = DATASTORE.resolve("pd-test-invalid-xml")
        .toString();

    private DataFileTypeCrud dataFileTypeCrud = Mockito.mock(DataFileTypeCrud.class);
    private ModelCrud modelCrud = Mockito.mock(ModelCrud.class);

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    // Basic functionality -- multiple files, multiple definitions, get imported
    @Test
    public void testBasicImport() throws JAXBException {

        DataFileTypeImporter dataFileImporter = new DataFileTypeImporter(
            ImmutableList.of(FILE_1, FILE_2), false);
        DataFileTypeImporter importerSpy = Mockito.spy(dataFileImporter);
        Mockito.when(importerSpy.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(importerSpy.modelCrud()).thenReturn(modelCrud);
        importerSpy.importFromFiles();

        assertEquals(6, importerSpy.getDataFileImportedCount());
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());

        assertEquals(2, importerSpy.getModelFileImportedCount());
        Mockito.verify(modelCrud, Mockito.times(1)).persist(ArgumentMatchers.<ModelType> anyList());
    }

    // Dry run test -- should import but not persist
    @Test
    public void testDryRun() throws JAXBException {

        DataFileTypeImporter dataFileImporter = new DataFileTypeImporter(
            ImmutableList.of(FILE_1, FILE_2), true);
        DataFileTypeImporter importerSpy = Mockito.spy(dataFileImporter);
        Mockito.when(importerSpy.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(importerSpy.modelCrud()).thenReturn(modelCrud);
        importerSpy.importFromFiles();

        assertEquals(6, importerSpy.getDataFileImportedCount());
        Mockito.verify(dataFileTypeCrud, Mockito.times(0))
            .persist(ArgumentMatchers.<DataFileType> anyList());
        assertEquals(2, importerSpy.getModelFileImportedCount());
        Mockito.verify(modelCrud, Mockito.times(0)).persist(ArgumentMatchers.<ModelType> anyList());
    }

    // Test with missing and non-regular files -- should still import from the present,
    // regular files
    @Test
    public void testWithInvalidFiles() throws JAXBException {

        DataFileTypeImporter dataFileImporter = new DataFileTypeImporter(
            ImmutableList.of(FILE_1, FILE_2, NO_SUCH_FILE, NOT_REGULAR_FILE), false);
        DataFileTypeImporter importerSpy = Mockito.spy(dataFileImporter);
        Mockito.when(importerSpy.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(importerSpy.modelCrud()).thenReturn(modelCrud);
        importerSpy.importFromFiles();

        assertEquals(6, importerSpy.getDataFileImportedCount());
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());
    }

    // Test with a file that has an entry that is valid XML but instantiates to an
    // invalid DataFileType instance
    @Test
    public void testWithInvalidDataFileType() throws JAXBException {

        DataFileTypeImporter dataFileImporter = new DataFileTypeImporter(
            ImmutableList.of(FILE_1, INVALID_FILE_1), false);
        DataFileTypeImporter importerSpy = Mockito.spy(dataFileImporter);
        Mockito.when(importerSpy.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(importerSpy.modelCrud()).thenReturn(modelCrud);
        importerSpy.importFromFiles();

        assertEquals(5, importerSpy.getDataFileImportedCount());
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());
    }

    // Test with a file that has an entry that is invalid XML
    @Test
    public void testWithInvalidDataXml() throws JAXBException {

        DataFileTypeImporter dataFileImporter = new DataFileTypeImporter(
            ImmutableList.of(FILE_1, INVALID_FILE_2), false);
        DataFileTypeImporter importerSpy = Mockito.spy(dataFileImporter);
        Mockito.when(importerSpy.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(importerSpy.modelCrud()).thenReturn(modelCrud);
        importerSpy.importFromFiles();

        assertEquals(5, importerSpy.getDataFileImportedCount());
        Mockito.verify(dataFileTypeCrud, Mockito.times(1))
            .persist(ArgumentMatchers.<DataFileType> anyList());
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateNames() throws JAXBException {

        DataFileTypeImporter dataFileImporter = new DataFileTypeImporter(
            ImmutableList.of(FILE_1, FILE_1), false);
        DataFileTypeImporter importerSpy = Mockito.spy(dataFileImporter);
        Mockito.when(importerSpy.dataFileTypeCrud()).thenReturn(dataFileTypeCrud);
        Mockito.when(importerSpy.modelCrud()).thenReturn(modelCrud);
        importerSpy.importFromFiles();
    }
}
