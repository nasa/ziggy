package gov.nasa.ziggy;

/**
 * Marker interface for tests that don't fail every time. To use mark the class or method with
 *
 * <pre>
 * {@literal @}Category (FlakyTestCategory.class)
 * </pre>
 *
 * These tests are implicitly marked with {@link IntegrationTestCategory}. This means that tests
 * that have this marker don't need an additional {@code IntegrationTestCategory} marker, nor does
 * {@code FlakyTestCategory} have to be added to a list of categories in a build file that already
 * excludes {@code IntegrationTestCategory}. However, this marker should probably be excluded by
 * Gradle integration test tasks that include the {@code IntegrationTestCategory}.
 *
 * @author Bill Wohler
 */
public interface FlakyTestCategory extends IntegrationTestCategory {
}
