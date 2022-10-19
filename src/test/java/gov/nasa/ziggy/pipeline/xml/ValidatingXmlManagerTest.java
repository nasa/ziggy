package gov.nasa.ziggy.pipeline.xml;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionFile;
import gov.nasa.ziggy.services.config.PropertyNames;
import jakarta.xml.bind.UnmarshalException;

/**
 * Unit tests for {@link ValidatingXmlManager} class.
 *
 * @author PT
 */
public class ValidatingXmlManagerTest {

    private File invalidXmlFile;
    private File validXmlFile;

    @Before
    public void setUp() {

        String workingDir = System.getProperty("user.dir");
        Path homeDirPath = Paths.get(workingDir, "build");
        System.setProperty(PropertyNames.ZIGGY_HOME_DIR_PROP_NAME, homeDirPath.toString());
        Path invalidXmlPath = Paths.get(workingDir, "test", "data", "configuration",
            "invalid-pipeline-definition.xml");
        invalidXmlFile = invalidXmlPath.toFile();
        Path validXmlPath = Paths.get(workingDir, "test", "data", "configuration",
            "pd-hyperion.xml");
        validXmlFile = validXmlPath.toFile();
    }

    @After
    public void tearDown() throws IOException {
        System.clearProperty(PropertyNames.ZIGGY_HOME_DIR_PROP_NAME);
        FileUtils.deleteDirectory(new File(Filenames.BUILD_TEST));
    }

    @Test(expected = UnmarshalException.class)
    public void testUnmarshalInvalidXml()
			throws InstantiationException, IllegalAccessException, SAXException,
			jakarta.xml.bind.JAXBException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
			SecurityException {

        ValidatingXmlManager<PipelineDefinitionFile> xmlManager = new ValidatingXmlManager<>(
            PipelineDefinitionFile.class);
        xmlManager.unmarshal(invalidXmlFile);
    }

    @Test
    public void testUnmarshalValidXml()
			throws InstantiationException, IllegalAccessException, SAXException,
			jakarta.xml.bind.JAXBException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
			SecurityException {
        ValidatingXmlManager<PipelineDefinitionFile> xmlManager = new ValidatingXmlManager<>(
            PipelineDefinitionFile.class);
        xmlManager.unmarshal(validXmlFile);

    }

}
