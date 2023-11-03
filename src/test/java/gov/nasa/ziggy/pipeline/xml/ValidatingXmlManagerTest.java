package gov.nasa.ziggy.pipeline.xml;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionFile;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import jakarta.xml.bind.JAXBException;

/**
 * Unit tests for {@link ValidatingXmlManager} class.
 *
 * @author PT
 */
public class ValidatingXmlManagerTest {

    private File invalidXmlFile;
    private File validXmlFile;

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Before
    public void setUp() {
        Path invalidXmlPath = Paths.get("test", "data", "configuration",
            "invalid-pipeline-definition.xml");
        invalidXmlFile = invalidXmlPath.toFile();
        Path validXmlPath = Paths.get("test", "data", "configuration", "pd-hyperion.xml");
        validXmlFile = validXmlPath.toFile();
    }

    @Test(expected = PipelineException.class)
    public void testUnmarshalInvalidXml() throws InstantiationException, IllegalAccessException,
        SAXException, JAXBException, IllegalArgumentException, InvocationTargetException,
        NoSuchMethodException, SecurityException {

        ValidatingXmlManager<PipelineDefinitionFile> xmlManager = new ValidatingXmlManager<>(
            PipelineDefinitionFile.class);
        xmlManager.unmarshal(invalidXmlFile);
    }

    @Test
    public void testUnmarshalValidXml() throws InstantiationException, IllegalAccessException,
        SAXException, JAXBException, IllegalArgumentException, InvocationTargetException,
        NoSuchMethodException, SecurityException {
        ValidatingXmlManager<PipelineDefinitionFile> xmlManager = new ValidatingXmlManager<>(
            PipelineDefinitionFile.class);
        xmlManager.unmarshal(validXmlFile);
    }
}
