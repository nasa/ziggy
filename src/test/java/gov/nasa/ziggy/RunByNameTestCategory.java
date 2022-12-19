package gov.nasa.ziggy;

/**
 * Marker interface for tests that need to run by themselves. To use mark the class or method with
 *
 * <pre>
 * {@literal @}Category (RunByNameTestCategory.class)
 * </pre>
 *
 * These tests are implicitly marked with {@link IntegrationTestCategory}. This means that tests
 * that have this marker don't need an additional {@code IntegrationTestCategory} marker, nor does
 * {@code RunByNameTestCategory} have to be added to a list of categories in a build file that
 * already excludes {@code IntegrationTestCategory}. However, this marker should probably be
 * excluded by Gradle integration test tasks that include the {@code IntegrationTestCategory}.
 *
 * @author Bill Wohler
 */
public interface RunByNameTestCategory extends IntegrationTestCategory {
}
