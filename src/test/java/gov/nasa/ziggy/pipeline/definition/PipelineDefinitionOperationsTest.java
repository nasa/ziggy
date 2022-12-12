package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hibernate.Hibernate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.parameters.DefaultParameters;
import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;

/**
 * Implements unit tests for the {@link PipelineDefinitionOperations} class. Because of the
 * complexity of that class, unit tests are very spare.
 */
public class PipelineDefinitionOperationsTest {

    @Before
    public void setup() {
        System.setProperty("ziggy.home.dir",
            Paths.get(System.getProperty("user.dir"), "build").toString());
        ZiggyUnitTestUtils.setUpDatabase();
    }

    @After
    public void tearDown() {
        ZiggyUnitTestUtils.tearDownDatabase();
    }

    @Test
    public void testMultipleDefaultParamSets() throws Exception {

        // Define the path to the pipeline definitions directory
        Path pipelineDefsDir = Paths.get(System.getProperty("user.dir"), "test", "data",
            "classwrapper");

        // Read in the parameter library
        DatabaseTransactionFactory.performTransaction(() -> {
            new ParametersOperations().importParameterLibrary(
                new File(pipelineDefsDir.toFile(), "pl-two-default-param-sets.xml"), null,
                ParamIoMode.STANDARD);
            return null;
        });

        // Read in the pipeline definition that has 2 instances of DefaultParameters
        // attached to it.
        DatabaseTransactionFactory.performTransaction(() -> {
            new PipelineDefinitionOperations().importPipelineConfiguration(Sets
                .newHashSet(new File(pipelineDefsDir.toFile(), "pd-two-default-param-sets.xml")));
            return null;
        });

        // Retrieve the pipeline definition
        PipelineDefinition pipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition pd = new PipelineDefinitionCrud()
                    .retrieveLatestVersionForName("hyperion");
                Hibernate.initialize(pd.getPipelineParameterSetNames());
                return pd;
            });

        // Check the contents of the parameter set map.
        Map<ClassWrapper<Parameters>, ParameterSetName> parsMap = pipelineDef
            .getPipelineParameterSetNames();
        assertEquals(2, parsMap.size());
        for (Map.Entry<ClassWrapper<Parameters>, ParameterSetName> mapEntry : parsMap.entrySet()) {
            assertEquals("gov.nasa.ziggy.parameters.DefaultParameters",
                mapEntry.getKey().unmangledClassName());
            assertEquals(
                "gov.nasa.ziggy.parameters.DefaultParameters " + mapEntry.getValue().getName(),
                mapEntry.getKey().getClassName());

            // Make sure that newInstance() works.
            assertEquals(DefaultParameters.class, mapEntry.getKey().newInstance().getClass());
        }

        // Create a pipeline instance for this pipeline
        PipelineInstance pipelineInstance = (PipelineInstance) DatabaseTransactionFactory
            .performTransaction(() -> {
                return new PipelineExecutor().launch(pipelineDef, "instance-name", null, null)
                    .getPipelineInstance();
            });

        Map<ClassWrapper<Parameters>, ParameterSet> pipelineParameters = pipelineInstance
            .getPipelineParameterSets();
        assertEquals(2, pipelineParameters.size());

        // Check the values in the parameter sets
        checkParameterSetValues(pipelineParameters.values());
    }

    private void checkParameterSetValues(Collection<ParameterSet> parameterSets) {
        for (ParameterSet parameterSet : parameterSets) {
            DefaultParameters pars = (DefaultParameters) parameterSet.getParameters().getInstance();
            if (parameterSet.getName().getName().equals("Sample classless parameter set")) {
                assertEquals(3, pars.getParameters().size());
                assertEquals(100, pars.getParameter("z1").getValue());
                assertEquals("some text", pars.getParameter("z3").getValue());
                float[] floatArray = (float[]) pars.getParameter("z2").getValue();
                assertEquals(2, floatArray.length);
                assertEquals(28.56, floatArray[0], 1e-3);
                assertEquals(57.12, floatArray[1], 1e-3);
            } else {
                assertEquals("ISOFIT module parameters", parameterSet.getName().getName());
                assertEquals(4, pars.getParameters().size());
                assertEquals(4, pars.getParameter("n_cores").getValue());
                assertEquals(false, pars.getParameter("use_hyperthreading").getValue());
                assertEquals(true, pars.getParameter("presolve").getValue());
                assertEquals(true, pars.getParameter("empirical_line").getValue());
            }
        }
    }

    /**
     * Tests that whitespace is allowed around the list element delimiter.
     */
    @Test
    public void testSplitList() {
        verifyParsing("1", new String[] { "1" });
        verifyParsing("1,2", new String[] { "1", "2" });
        verifyParsing("1 ,2", new String[] { "1", "2" });
        verifyParsing("1, 2", new String[] { "1", "2" });
        verifyParsing("1  ,  2", new String[] { "1", "2" });
    }

    private void verifyParsing(String value, String[] elements) {
        List<String> expected = ImmutableList.copyOf(elements);
        List<String> actual = PipelineDefinitionOperations.splitAndListifyString(value);
        assertEquals(expected, actual);
    }

}
