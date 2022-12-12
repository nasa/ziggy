package gov.nasa.ziggy;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * Implements a {@link TestRule} for the setting and resetting of properties for use by unit tests.
 * To use, declare a field that refers to this rule as shown.
 *
 * <pre>
 * &#64;Rule
 * public ZiggyPropertyRule propPropertyRule = new ZiggyPropertyRule("prop", "value");
 * </pre>
 *
 * For convenience, this rule provides a {@link #getProperty} method to access the current property
 * and a {@link #getPreviousProperty} method to access the value of the property before the test
 * started.
 * <p>
 * This class is marked {@code @NotThreadSafe} as it manipulates system properties that are global
 * across the JVM.
 *
 * @author Bill Wohler
 */
@NotThreadSafe
public class ZiggyPropertyRule extends ExternalResource {

    private String property;
    private String value;
    private String previousValue;

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and value.
     *
     * @param property the non-{@code null} property to set
     * @param value the value to set the property to. This can be {@code null} to avoid setting the
     * property before each test, but still reset the property after each test. This is useful if a
     * test modifies the property.
     */
    public ZiggyPropertyRule(String property, String value) {
        this.property = checkNotNull(property, "property");
        this.value = value;
    }

    @Override
    protected void before() throws Throwable {
        if (value != null) {
            previousValue = System.setProperty(property, value);
        } else {
            previousValue = System.getProperty(property);
        }
    }

    @Override
    protected void after() {
        resetSystemProperty(property, previousValue);
    }

    /**
     * Sets the given property to the given value. If {@code value} is {@code null}, the property is
     * cleared.
     *
     * @param property the property to set
     * @param value the value to set the property to, or {@code null} to clear the property
     */
    public static void resetSystemProperty(String property, String value) {
        if (value != null) {
            System.setProperty(property, value);
        } else {
            System.clearProperty(property);
        }
    }
}
