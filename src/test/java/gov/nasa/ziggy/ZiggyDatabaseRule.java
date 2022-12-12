package gov.nasa.ziggy;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * Implements a {@link TestRule} for the set up and tear down of databases for use by unit tests. To
 * use, declare a field that refers to this rule as shown below. The test won't need to use that
 * field as database actions are typically performed within a
 * DatabaseTransactionFactory.performTransaction() lambda.
 *
 * <pre>
 * &#64;Rule
 * public ZiggyDatabaseRule databaseRule = new ZiggyDatabaseRule();
 * </pre>
 *
 * The {@code before()} method of this rule runs before the {@code @Before} method of the test so
 * that the database can be safely populated there.
 *
 * @author Bill Wohler
 */
public class ZiggyDatabaseRule extends ExternalResource {

    @Override
    protected void before() throws Throwable {
        ZiggyUnitTestUtils.setUpDatabase();
    }

    @Override
    protected void after() {
        ZiggyUnitTestUtils.tearDownDatabase();
    }
}
