package gov.nasa.ziggy.pipeline.definition.crud;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyDatabaseRule;
import gov.nasa.ziggy.ZiggyUnitTestUtils;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.definition.AuditInfo;
import gov.nasa.ziggy.pipeline.definition.BeanWrapper;
import gov.nasa.ziggy.pipeline.definition.ClassWrapper;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.TestModuleParameters;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.security.User;
import gov.nasa.ziggy.services.security.UserCrud;
import gov.nasa.ziggy.uow.SingleUnitOfWorkGenerator;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.ReflectionEquals;

/**
 * Tests for {@link PipelineDefinitionCrud} Tests that objects can be stored, retrieved, and edited
 * and that mapping metadata (associations, cascade rules, etc.) are setup correctly and work as
 * expected.
 *
 * @author Todd Klaus
 */
public class PipelineDefinitionCrudTest {
    private static final String TEST_PIPELINE_NAME_1 = "Test Pipeline 1";

    private UserCrud userCrud;

    private User adminUser;
    private User operatorUser;

    private PipelineDefinitionCrud pipelineDefinitionCrud;

    private PipelineModuleDefinitionCrud pipelineModuleDefinitionCrud;
    private ParameterSetCrud parameterSetCrud;

    private ParameterSet expectedParamSet;
    private PipelineModuleDefinition expectedModuleDef1;
    private PipelineModuleDefinition expectedModuleDef2;
    private PipelineModuleDefinition expectedModuleDef3;
    private ReflectionEquals comparer;

    @Rule
    public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();

    @Before
    public void setUp() {
        userCrud = new UserCrud();
        pipelineDefinitionCrud = new PipelineDefinitionCrud();
        pipelineModuleDefinitionCrud = new PipelineModuleDefinitionCrud();
        parameterSetCrud = new ParameterSetCrud();
        comparer = new ReflectionEquals();
        comparer.excludeField(".*\\.lastChangedTime");
        comparer.excludeField(".*\\.lastChangedUser.created");
        comparer.excludeField(".*\\.uowProperties.instance");
        comparer.excludeField(".*\\.nodesAndParamSets");
        comparer.excludeField(".*\\.childNodeNames");
        comparer.excludeField(".*\\.rootNodeNames");
    }

    private PipelineDefinition populateObjects() {

        PipelineDefinition pipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {

                // create users
                adminUser = new User("admin", "Administrator", "admin@example.com", "x111");
                userCrud.createUser(adminUser);

                operatorUser = new User("ops", "Operator", "ops@example.com", "x112");
                userCrud.createUser(operatorUser);

                // create a module param set def
                expectedParamSet = new ParameterSet(new AuditInfo(adminUser, new Date()),
                    "test mps1");
                expectedParamSet
                    .setParameters(new BeanWrapper<Parameters>(new TestModuleParameters()));
                parameterSetCrud.create(expectedParamSet);

                // create a few module defs
                expectedModuleDef1 = new PipelineModuleDefinition("Test-1");
                pipelineModuleDefinitionCrud.create(expectedModuleDef1);

                expectedModuleDef2 = new PipelineModuleDefinition("Test-2");
                pipelineModuleDefinitionCrud.create(expectedModuleDef2);

                expectedModuleDef3 = new PipelineModuleDefinition("Test-3");
                pipelineModuleDefinitionCrud.create(expectedModuleDef3);

                // create a pipeline def
                PipelineDefinition pd = createPipelineDefinition();
                pipelineDefinitionCrud.create(pd);

                return pd;
            });
        return pipelineDef;
    }

    private PipelineDefinition createPipelineDefinition() {
        PipelineDefinition pipelineDef = new PipelineDefinition(
            new AuditInfo(adminUser, new Date()), TEST_PIPELINE_NAME_1);
        PipelineDefinitionNode pipelineNode1 = new PipelineDefinitionNode(
            expectedModuleDef1.getName(), pipelineDef.getName().getName());
        PipelineDefinitionNode pipelineNode2 = new PipelineDefinitionNode(
            expectedModuleDef2.getName(), pipelineDef.getName().getName());
        pipelineNode1.getNextNodes().add(pipelineNode2);

        pipelineNode1.setUnitOfWorkGenerator(
            new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
        pipelineNode1.setStartNewUow(false);

        pipelineNode2.setUnitOfWorkGenerator(
            new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
        pipelineNode2.setStartNewUow(false);

        pipelineDef.addRootNode(pipelineNode1);

        return pipelineDef;
    }

    private int pipelineNodeCount() {
        DatabaseService databaseService = DatabaseService.getInstance();
        Query q = databaseService.getSession()
            .createQuery("select count(*) from PipelineDefinitionNode");
        int count = ((Long) q.uniqueResult()).intValue();

        return count;
    }

    private int pipelineModuleDefinitionCount() {
        DatabaseService databaseService = DatabaseService.getInstance();
        Query q = databaseService.getSession()
            .createQuery("select count(*) from PipelineModuleDefinition");
        int count = ((Long) q.uniqueResult()).intValue();

        return count;
    }

    private int pipelineModuleParamSetCount() {
        DatabaseService databaseService = DatabaseService.getInstance();
        Query q = databaseService.getSession().createQuery("select count(*) from ParameterSet");
        int count = ((Long) q.uniqueResult()).intValue();

        return count;
    }

    /**
     * Stores a new PipelineDefinition in the db, then retrieves it and makes sure it matches what
     * was put in
     *
     * @throws Exception
     */
    @Test
    public void testStoreAndRetrieve() throws Exception {

        PipelineDefinition expectedPipelineDef = populateObjects();

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;

            });
        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        List<PipelineDefinition> latestVersions = pipelineDefinitionCrud.retrieveLatestVersions();
        assertEquals("latestVersions count", 1, latestVersions.size());
        comparer.assertEquals("latest version", expectedPipelineDef, latestVersions.get(0));

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    @Test
    public void testEditPipelineDefinition() throws Exception {
        // Create
        populateObjects();

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDef(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        // Create & Edit
        PipelineDefinition expectedPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition epd = createPipelineDefinition();
                editPipelineDef(epd);
                epd.setDirty(1);
                ZiggyUnitTestUtils.initializePipelineDefinition(epd);
                return epd;
            });
        // flush changes

        // databaseService.closeCurrentSession(); // clear the cache ,
        // detach
        // the objects

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user
     *
     * @param pipelineDef
     */
    private void editPipelineDef(PipelineDefinition pipelineDef) {
        pipelineDef.setDescription("new description");
        pipelineDef.getAuditInfo().setLastChangedTime(new Date());
        pipelineDef.getAuditInfo().setLastChangedUser(operatorUser);
    }

    @Test
    public void testEditPipelineDefinitionAddNextNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefAddNextNode(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefAddNextNode(expectedPipelineDef);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 3, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user add a new node after the last node: N1 -> N2 -> N3(new)
     *
     * @param pipelineDef
     * @throws PipelineException
     */
    private void editPipelineDefAddNextNode(PipelineDefinition pipelineDef) {
        PipelineDefinitionNode newPipelineNode = new PipelineDefinitionNode(
            expectedModuleDef3.getName(), pipelineDef.getName().getName());
        pipelineDef.getRootNodes().get(0).getNextNodes().get(0).getNextNodes().add(newPipelineNode);
        newPipelineNode.setUnitOfWorkGenerator(
            new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
        newPipelineNode.setStartNewUow(false);
    }

    @Test
    public void testEditPipelineDefinitionAddBranchNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {

            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefAddBranchNode(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefAddBranchNode(expectedPipelineDef);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 3, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user add a new node branch off the second node: N1 -> N2 \>
     * N3(new)
     *
     * @param pipelineDef
     * @throws PipelineException
     */
    private void editPipelineDefAddBranchNode(PipelineDefinition pipelineDef) {
        PipelineDefinitionNode newPipelineNode = new PipelineDefinitionNode(
            expectedModuleDef3.getName(), pipelineDef.getName().getName());
        pipelineDef.getRootNodes().get(0).getNextNodes().add(newPipelineNode);
        newPipelineNode.setUnitOfWorkGenerator(
            new ClassWrapper<UnitOfWorkGenerator>(new SingleUnitOfWorkGenerator()));
        newPipelineNode.setStartNewUow(false);
    }

    @Test
    public void testEditPipelineDefinitionChangeNodeModule() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefChangeNodeModule(modifiedPipelineDef);
            return null;
        });
        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefChangeNodeModule(expectedPipelineDef);
        expectedPipelineDef.populateXmlFields();

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 2, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user change node module for first node from module1 to
     * module3
     *
     * @param pipelineDef
     */
    private void editPipelineDefChangeNodeModule(PipelineDefinition pipelineDef) {
        pipelineDef.getRootNodes().get(0).setModuleName(expectedModuleDef3.getName());
    }

    @Test
    public void testEditPipelineDefinitionDeleteLastNode() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefDeleteLastNode(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefDeleteLastNode(expectedPipelineDef);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 1, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user delete last node
     *
     * @param pipelineDef
     */
    private void editPipelineDefDeleteLastNode(PipelineDefinition pipelineDef) {
        List<PipelineDefinitionNode> nextNodes = pipelineDef.getRootNodes().get(0).getNextNodes();

        for (PipelineDefinitionNode nextNode : nextNodes) {
            pipelineDefinitionCrud.delete(nextNode);
        }
        nextNodes.clear();
    }

    @Test
    public void testEditPipelineDefinitionDeleteAllNodes() throws Exception {
        // Create
        populateObjects();

        // Retrieve & Edit
        DatabaseTransactionFactory.performTransaction(() -> {
            PipelineDefinition modifiedPipelineDef = pipelineDefinitionCrud
                .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
            editPipelineDefDeleteAllNodes(modifiedPipelineDef);
            return null;
        });

        // Retrieve
        PipelineDefinition actualPipelineDef = (PipelineDefinition) DatabaseTransactionFactory
            .performTransaction(() -> {
                PipelineDefinition apd = pipelineDefinitionCrud
                    .retrieveLatestVersionForName(TEST_PIPELINE_NAME_1);
                ZiggyUnitTestUtils.initializePipelineDefinition(apd);
                return apd;
            });

        PipelineDefinition expectedPipelineDef = createPipelineDefinition();
        editPipelineDefDeleteAllNodes(expectedPipelineDef);
        expectedPipelineDef.setDirty(1);

        comparer.excludeField(".*\\.id");

        comparer.assertEquals("PipelineDefinition", expectedPipelineDef, actualPipelineDef);

        assertEquals("PipelineDefinitionNode count", 0, pipelineNodeCount());
        assertEquals("PipelineModuleDefinition count", 3, pipelineModuleDefinitionCount());
        assertEquals("ParameterSet count", 1, pipelineModuleParamSetCount());
    }

    /**
     * simulate modifications made by a user delete all nodes
     *
     * @param pipelineDef
     */
    private void editPipelineDefDeleteAllNodes(PipelineDefinition pipelineDef) {
        pipelineDefinitionCrud.deleteAllPipelineNodes(pipelineDef);
    }

    @Test
    public void testRetrievePipelineDefinitionNamesInUse() throws Exception {
        // No pipeline definitions at all. Should be empty.
        assertEquals(0, pipelineDefinitionCrud.retrievePipelineDefinitionNamesInUse().size());

        // Add a pipeline definition, but without an associated pipeline
        // instances. Should return an empty list.
        // Create
        PipelineDefinition pipelineDefinition = populateObjects();

        assertEquals(0, pipelineDefinitionCrud.retrievePipelineDefinitionNamesInUse().size());

        // Now, create a pipeline instance associated with the pipeline
        // definition. Should return a single item.
        PipelineInstance pipelineInstance = new PipelineInstance(pipelineDefinition);
        PipelineInstanceCrud pipelineInstanceCrud = new PipelineInstanceCrud();

        DatabaseTransactionFactory.performTransaction(() -> {
            pipelineInstanceCrud.create(pipelineInstance);
            return null;
        });

        List<String> pipelineDefinitions = pipelineDefinitionCrud
            .retrievePipelineDefinitionNamesInUse();

        assertEquals(1, pipelineDefinitions.size());

        String name = pipelineDefinitions.get(0);
        assertEquals(TEST_PIPELINE_NAME_1, name);
    }
}
