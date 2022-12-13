package gov.nasa.ziggy;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/*
 * Idea: If multiple properties need to be set, perhaps it would be more convenient to put them in
 * one rule. If so, add constructors that take multiple name/value pairs as varargs or a map. In
 * this case, add a getProperty(String) method to return the current value of the given property. I
 * imagine that the existing getProperty() method should throw an IllegalStateException if more than
 * one property is defined. This would then make a nice superclass for subclasses with a number of
 * predefined properties.
 */

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
 * value and a {@link #getPreviousProperty} method to access the value of the property before the
 * test started.
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
     * @param value the value to set the property to. This can be {@code null} to clear the property
     * before each test, and reset the property after each test. This is useful if a test modifies
     * the property or depends on the property being cleared.
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
            System.clearProperty(property);
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

    /**
     * Returns the value of the property set by this rule.
     */
    public String getProperty() {
        return value;
    }

    /**
     * Returns the previous value of the property before it was set by this rule.
     */
    public String getPreviousProperty() {
        return previousValue;
    }
}
