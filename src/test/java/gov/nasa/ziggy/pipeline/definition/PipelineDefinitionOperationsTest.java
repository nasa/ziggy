package gov.nasa.ziggy.pipeline.definition;

import static gov.nasa.ziggy.services.config.PropertyName.ZIGGY_HOME_DIR;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hibernate.Hibernate;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.parameters.ParameterLibraryImportExportCli.ParamIoMode;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.parameters.ParametersInterface;
import gov.nasa.ziggy.parameters.ParametersOperations;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineDefinitionCrud;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;

/**
 * Implements unit tests for the {@link PipelineDefinitionOperations} class. Because of the
 * complexity of that class, unit tests are very spare.
 */
public class PipelineDefinitionOperationsTest {

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Rule
    public ZiggyPropertyRule ziggyHomeDirPropertyRule = new ZiggyPropertyRule(ZIGGY_HOME_DIR,
        DirectoryProperties.ziggyCodeBuildDir().toString());

    @Test
    public void testMultipleDefaultParamSets() throws Exception {

        // Use the PipelineSupervisor constructor to set the worker count.
        new PipelineSupervisor(1, 1000);
        // Define the path to the pipeline definitions directory
        Path pipelineDefsDir = ZiggyUnitTestUtils.TEST_DATA.resolve("classwrapper");

        // Read in the parameter library
        DatabaseTransactionFactory.performTransaction(() -> {
            new ParametersOperations().importParameterLibrary(
                new File(pipelineDefsDir.toFile(), "pl-two-default-param-sets.xml"), null,
                ParamIoMode.STANDARD);
            return null;
        });

        // Read in the pipeline definition that has 2 instances of Parameters
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
        Map<ClassWrapper<ParametersInterface>, String> parsMap = pipelineDef
            .getPipelineParameterSetNames();
        assertEquals(2, parsMap.size());
        for (Map.Entry<ClassWrapper<ParametersInterface>, String> mapEntry : parsMap.entrySet()) {
            assertEquals("gov.nasa.ziggy.parameters.Parameters",
                mapEntry.getKey().unmangledClassName());
            assertEquals("gov.nasa.ziggy.parameters.Parameters " + mapEntry.getValue(),
                mapEntry.getKey().getClassName());

            // Make sure that newInstance() works.
            assertEquals(Parameters.class, mapEntry.getKey().newInstance().getClass());
        }

        // Create a pipeline instance for this pipeline
        PipelineInstance pipelineInstance = new PipelineExecutor().launch(pipelineDef,
            "instance-name", null, null, null);

        Map<ClassWrapper<ParametersInterface>, ParameterSet> pipelineParameters = pipelineInstance
            .getPipelineParameterSets();
        assertEquals(2, pipelineParameters.size());

        // Check the values in the parameter sets
        checkParameterSetValues(pipelineParameters.values());
    }

    private void checkParameterSetValues(Collection<ParameterSet> parameterSets) {
        for (ParameterSet parameterSet : parameterSets) {
            Parameters parameters = new Parameters();
            parameters.setParameters(parameterSet.getTypedParameters());
            if (parameterSet.getName().equals("Sample classless parameter set")) {
                assertEquals(3, parameters.getParameters().size());
                assertEquals(100, parameters.getParameter("z1").getValue());
                assertEquals("some text", parameters.getParameter("z3").getValue());
                float[] floatArray = (float[]) parameters.getParameter("z2").getValue();
                assertEquals(2, floatArray.length);
                assertEquals(28.56, floatArray[0], 1e-3);
                assertEquals(57.12, floatArray[1], 1e-3);
            } else {
                assertEquals("ISOFIT module parameters", parameterSet.getName());
                assertEquals(4, parameters.getParameters().size());
                assertEquals(4, parameters.getParameter("n_cores").getValue());
                assertEquals(false, parameters.getParameter("use_hyperthreading").getValue());
                assertEquals(true, parameters.getParameter("presolve").getValue());
                assertEquals(true, parameters.getParameter("empirical_line").getValue());
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
