package gov.nasa.ziggy.pipeline.definition.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ReflectionEquals;
import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.collections.ZiggyDataType;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.management.DataReceiptPipelineStepExecutor;
import gov.nasa.ziggy.pipeline.definition.Parameter;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
import gov.nasa.ziggy.services.database.DatabaseOperations;
import gov.nasa.ziggy.uow.DataReceiptUnitOfWorkGenerator;
import gov.nasa.ziggy.util.PipelineException;

/**
 * Unit tests for {@link PipelineStepOperations} and {@link PipelineStepCrud}.
 */
public class PipelineStepOperationsTest {

    private PipelineStepOperations pipelineStepOperations;
    private PipelineOperationsTestUtils pipelineOperationsTestUtils = new PipelineOperationsTestUtils();
    private TestOperations testOperations = new TestOperations();
    private ParameterSetCrud parameterSetCrud;
    private PipelineStepCrud pipelineStepCrud;
    private static final String TEST_PIPELINE_STEP_NAME_1 = "Test Pipeline Step 1";
    private static final String TEST_PARAM_SET_NAME_1 = "Test MPS-1";
    private static final String MISSING_PIPELINE_STEP = "I DONT EXIST";
    private ReflectionEquals comparer;
    private ParametersOperations parametersOperations = new ParametersOperations();

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        pipelineStepOperations = new PipelineStepOperations();
        parameterSetCrud = new ParameterSetCrud();
        pipelineStepCrud = new PipelineStepCrud();
        parameterSetCrud = new ParameterSetCrud();
        comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.id");
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.lastChangedUser.created");
        comparer.excludeField(".*\\.xmlParameters");
        comparer.excludeField(".*\\.pipelineInstanceNodes");
        comparer.excludeField(".*\\.pipelineNodes");
    }

    @Test
    public void testLatestPipelineStepVersionForName() {
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        PipelineStep pipelineStep = pipelineStepOperations.pipelineStep("step1");
        assertEquals("description", pipelineStep.getDescription());

        // Make a change, persist, and see if the retrieved version is correct.
        pipelineStep.setDescription("Updated version");
        testOperations.merge(pipelineStep);
        PipelineStep pipelineStep2 = pipelineStepOperations.pipelineStep("step1");
        assertEquals("Updated version", pipelineStep2.getDescription());
        assertFalse(pipelineStep2.isLocked());
        assertEquals(0, pipelineStep2.getVersion());

        // Lock the current version.
        pipelineStep2.lock();
        PipelineStep pipelineStep3 = testOperations.merge(pipelineStep2);

        // Make a change, persist, and see if the retrieved version is correct (i.e., is unlocked
        // and has its version number incremented).
        pipelineStep3.setDescription("Even more updated version");
        testOperations.merge(pipelineStep3);
        PipelineStep pipelineStep4 = pipelineStepOperations.pipelineStep("step1");
        assertEquals("Even more updated version", pipelineStep4.getDescription());
        assertFalse(pipelineStep4.isLocked());
        assertEquals(1, pipelineStep4.getVersion());
    }

    @Test
    public void testCreateDataReceiptPipelineStepExecutor() {
        pipelineStepOperations.createDataReceiptPipelineStep();
        PipelineStep dataReceiptStep = pipelineStepOperations
            .pipelineStep(DataReceiptPipelineStepExecutor.DATA_RECEIPT_PIPELINE_STEP_EXECUTOR_NAME);
        assertNotNull(dataReceiptStep);
        assertEquals(DataReceiptUnitOfWorkGenerator.class,
            dataReceiptStep.getUnitOfWorkGenerator().getClazz());
    }

    private PipelineStep populateObjects() {
        ParameterSet paramSet = createParameterSet(TEST_PARAM_SET_NAME_1);
        testOperations.mergeParameterSet(paramSet);
        PipelineStep pipelineStep = createPipelineStep();
        return testOperations.mergePipelineStep(pipelineStep);
    }

    private ParameterSet createParameterSet(String name) {
        ParameterSet parameterSet = new ParameterSet(name);
        Parameter parameter = new Parameter("value", "42", ZiggyDataType.ZIGGY_INT);
        parameterSet.getParameters().add(parameter);
        return parameterSet;
    }

    private PipelineStep createPipelineStep() {
        return new PipelineStep(TEST_PIPELINE_STEP_NAME_1);
    }

    private int pipelineStepCount() {
        ZiggyQuery<PipelineStep, Long> query = pipelineStepCrud.createZiggyQuery(PipelineStep.class,
            Long.class);
        Long count = pipelineStepCrud.uniqueResult(query.count());
        return count.intValue();
    }

    private int parameterSetCount() {
        ZiggyQuery<ParameterSet, Long> query = parameterSetCrud.createZiggyQuery(ParameterSet.class,
            Long.class);
        Long count = parameterSetCrud.uniqueResult(query.count());
        return count.intValue();
    }

    PipelineStep copy(PipelineStep original) throws NoSuchFieldException, SecurityException,
        IllegalArgumentException, IllegalAccessException {
        PipelineStep copy = new PipelineStep(original.getName());
        copy.setDescription(original.getDescription());
        copy.setPipelineStepExecutorClass(original.getPipelineStepExecutorClass());
        Field versionField = original.getClass().getSuperclass().getDeclaredField("version");
        versionField.setAccessible(true);
        versionField.set(copy, original.getVersion());
        setOptimisticLockValue(copy, original.getOptimisticLockValue());
        Field idField = original.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(copy, original.getId());
        return copy;
    }

    private void setOptimisticLockValue(PipelineStep pipelineStep, int dirty)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field dirtyField = pipelineStep.getClass()
            .getSuperclass()
            .getDeclaredField("optimisticLockValue");
        dirtyField.setAccessible(true);
        dirtyField.set(pipelineStep, dirty);
    }

    private void setOptimisticLockValue(ParameterSet parameterSet, int dirty)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException,
        IllegalAccessException {
        Field dirtyField = parameterSet.getClass()
            .getSuperclass()
            .getDeclaredField("optimisticLockValue");
        dirtyField.setAccessible(true);
        dirtyField.set(parameterSet, dirty);
    }

    private PipelineStep editPipelineStep(PipelineStep pipelineStep) {
        return editPipelineStep(pipelineStep, true);
    }

    /**
     * Simulate modifications made by a user.
     */
    private PipelineStep editPipelineStep(PipelineStep pipelineStep, boolean merge) {
        if (merge) {
            return testOperations.updatePipelineStep(pipelineStep.getName());
        }
        pipelineStep.setDescription("new description");
        pipelineStep.updateAuditInfo();
        return pipelineStep;
    }

    @Test
    public void testStoreAndRetrieve() throws Exception {
        PipelineStep expectedPipelineStep = populateObjects();

        // Retrieve
        PipelineStep actualPipelineStep = pipelineStepOperations
            .pipelineStep(TEST_PIPELINE_STEP_NAME_1);

        comparer.assertEquals("PipelineStep", expectedPipelineStep, actualPipelineStep);

        assertEquals("PipelineStep count", 1, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    @Test
    public void testRetrieveMissing() {
        PipelineStep pipelineStep = pipelineStepCrud
            .retrieveLatestVersionForName(MISSING_PIPELINE_STEP);

        assertNull("missing pipeline step", pipelineStep);
    }

    @Test(expected = PipelineException.class)
    public void testOptimisticLocking() {
        // Create
        PipelineStep pipelineStep = populateObjects();

        // Retrieve & Edit
        PipelineStep modifiedPipelineStep = pipelineStepOperations
            .pipelineStep(pipelineStep.getName());
        modifiedPipelineStep = editPipelineStep(modifiedPipelineStep);

        // Attempting to save the original version will fail because the save of a
        // modified version causes the original to be out of date.
        testOperations.mergePipelineStep(pipelineStep);
    }

    @Test
    public void testEditPipelineStep() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        PipelineStep pipelineStep = pipelineStepOperations.pipelineStep(TEST_PIPELINE_STEP_NAME_1);
        editPipelineStep(pipelineStep);

        // Retrieve
        PipelineStep actualPipelineStep = pipelineStepOperations
            .pipelineStep(TEST_PIPELINE_STEP_NAME_1);

        createParameterSet(TEST_PARAM_SET_NAME_1);
        PipelineStep expectedPipelineStep = createPipelineStep();
        expectedPipelineStep = editPipelineStep(expectedPipelineStep, false);
        setOptimisticLockValue(expectedPipelineStep, 1);

        comparer.assertEquals("PipelineStep", expectedPipelineStep, actualPipelineStep);
        assertEquals(1, actualPipelineStep.getOptimisticLockValue());

        assertEquals("PipelineStep count", 1, pipelineStepCount());
        assertEquals("ParameterSet count", 1, parameterSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    @Test
    public void testEditPipelineNodeParameterSetChangeParam() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        List<ParameterSet> modifiedParamSets = testOperations
            .allParameterSetVersions(TEST_PARAM_SET_NAME_1);
        assertEquals("paramSets size", 1, modifiedParamSets.size());
        ParameterSet modifiedParamSet = modifiedParamSets.get(0);
        editParamSetChangeParam(modifiedParamSet);

        // Retrieve
        List<ParameterSet> actualParamSets = testOperations
            .allParameterSetVersions(TEST_PARAM_SET_NAME_1);
        assertEquals("paramSets size", 1, actualParamSets.size());
        ParameterSet actualParamSet = actualParamSets.get(0);

        ParameterSet expectedParamSet = createParameterSet(TEST_PARAM_SET_NAME_1);
        editParamSetChangeParam(expectedParamSet, false);
        setOptimisticLockValue(expectedParamSet, 1);

        comparer.assertEquals("ParameterSet", expectedParamSet, actualParamSet);

        assertEquals("ParameterSet count", 1, parameterSetCount());
        assertEquals("ParameterSetName count", 1, paramSetNameCount());
    }

    private ParameterSet editParamSetChangeParam(ParameterSet paramSet) {
        return editParamSetChangeParam(paramSet, true);
    }

    /**
     * Simulate modifications made by a user.
     */
    private ParameterSet editParamSetChangeParam(ParameterSet paramSet, boolean merge) {
        if (merge) {
            return testOperations.updateTestParameters(paramSet.getName());
        }
        Parameter parameter = new Parameter("value", "100", ZiggyDataType.ZIGGY_INT);
        paramSet.getParameters().clear();
        paramSet.getParameters().add(parameter);
        return paramSet;
    }

    @Test
    public void testCreateOrUpdateDuplicateInstance() throws NoSuchFieldException,
        SecurityException, IllegalArgumentException, IllegalAccessException {
        PipelineStep pipelineStep = populateObjects();
        pipelineStep.setDescription("New description");
        testOperations.mergePipelineStep(pipelineStep);

        List<PipelineStep> pipelineSteps0 = testOperations.allPipelineSteps(pipelineStep.getName());
        assertEquals(1, pipelineSteps0.size());
        assertEquals("New description", pipelineSteps0.get(0).getDescription());

        // Executing with a different object does the wrong thing
        testOperations.mergePipelineStep(copy(pipelineSteps0.get(0)));

        List<PipelineStep> pipelineSteps = testOperations.allPipelineSteps(pipelineStep.getName());
        if (pipelineSteps.size() == 2) {
            assertEquals(pipelineSteps.get(0).getName(), pipelineSteps.get(1).getName());
            assertEquals(pipelineSteps.get(0).getVersion(), pipelineSteps.get(1).getVersion());
            assertFalse(pipelineSteps.get(0).isLocked());
            assertFalse(pipelineSteps.get(1).isLocked());
        }

        // Here's where we fail, because this should not have created a duplicate object
        assertEquals(1, pipelineSteps.size());
    }

    @Test
    public void testDeleteParameterSet() throws Exception {
        // Create
        populateObjects();

        assertEquals("ParameterSetName count", 1, paramSetNameCount());

        PipelineStep deletedPipelineStep = pipelineStepOperations
            .pipelineStep(TEST_PIPELINE_STEP_NAME_1);
        testOperations.removePipelineStep(deletedPipelineStep);

        ParameterSet deletedParamSet = parametersOperations.parameterSet(TEST_PARAM_SET_NAME_1);
        testOperations.removeParameterSet(deletedParamSet);

        assertEquals("ParameterSet count", 0, parameterSetCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("ParameterSetName count", 0, paramSetNameCount());
    }

    @Test
    public void testDeletePipelineStep() throws Exception {
        // Create
        populateObjects();

        assertEquals("PipelineStepName count", 1, pipelineStepNameCount());
        PipelineStep deletedPipelineStep = pipelineStepOperations
            .pipelineStep(TEST_PIPELINE_STEP_NAME_1);
        testOperations.removePipelineStep(deletedPipelineStep);

        assertEquals("PipelineStep count", 0, pipelineStepCount());
        // verify CascadeType.DELETE_ORPHAN functionality
        assertEquals("PipelineStepName count", 0, pipelineStepNameCount());
    }

    // For some reason, I'm not able to do the following two count queries as proper queries
    // using count(), but I'm able to retrieve all distinct names and return the size of the
    // resulting list.
    private int paramSetNameCount() {
        return testOperations.parameterSetNameCount();
    }

    private int pipelineStepNameCount() {
        return testOperations.pipelineStepNameCount();
    }

    @Test
    public void testLock() {
        pipelineOperationsTestUtils.setUpSingleNodePipeline();
        PipelineStep pipelineStep = pipelineStepOperations.pipelineStep("step1");
        assertFalse(pipelineStep.isLocked());
        pipelineStepOperations.lock(pipelineStep);
        pipelineStep = pipelineStepOperations.pipelineStep("step1");
        assertTrue(pipelineStep.isLocked());
    }

    private class TestOperations extends DatabaseOperations {

        public PipelineStep merge(PipelineStep pipelineStep) {
            return performTransaction(() -> new PipelineStepCrud().merge(pipelineStep));
        }

        public List<ParameterSet> allParameterSetVersions(String name) {
            return performTransaction(
                () -> new ParameterSetCrud().retrieveAllVersionsForName(name));
        }

        public void removePipelineStep(PipelineStep pipelineStep) {
            performTransaction(() -> new PipelineStepCrud().remove(pipelineStep));
        }

        public void removeParameterSet(ParameterSet parameterSet) {
            performTransaction(() -> new ParameterSetCrud().remove(parameterSet));
        }

        public List<PipelineStep> allPipelineSteps(String name) {
            return performTransaction(
                () -> new PipelineStepCrud().retrieveAllVersionsForName(name));
        }

        public PipelineStep mergePipelineStep(PipelineStep pipelineStep) {
            return performTransaction(() -> new PipelineStepCrud().merge(pipelineStep));
        }

        // Apparently this is the only way to get the Hibernate version to update:
        // the instance getting the update cannot become detached between its
        // retrieval and its persistence.
        public ParameterSet updateTestParameters(String parameterSetName) {
            return performTransaction(() -> {
                ParameterSet databaseParameterSet = new ParameterSetCrud()
                    .retrieveLatestVersionForName(parameterSetName);
                Parameter parameter = new Parameter("value", "100", ZiggyDataType.ZIGGY_INT);
                databaseParameterSet.getParameters().clear();
                databaseParameterSet.getParameters().add(parameter);
                return new ParameterSetCrud().merge(databaseParameterSet);
            });
        }

        public PipelineStep updatePipelineStep(String pipelineStepName) {
            return performTransaction(() -> {
                PipelineStep pipelineStep = new PipelineStepCrud()
                    .retrieveLatestVersionForName(pipelineStepName);
                pipelineStep.setDescription("new description");
                pipelineStep.updateAuditInfo();
                return new PipelineStepCrud().merge(pipelineStep);
            });
        }

        public int parameterSetNameCount() {
            return performTransaction(() -> new ParameterSetCrud().retrieveNames().size());
        }

        public int pipelineStepNameCount() {
            return performTransaction(() -> new PipelineStepCrud().retrieveNames().size());
        }

        public ParameterSet mergeParameterSet(ParameterSet parameterSet) {
            return performTransaction(() -> new ParameterSetCrud().merge(parameterSet));
        }
    }
}
