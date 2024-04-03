package gov.nasa.ziggy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.file.Path;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

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
 * To use, declare a field that refers to this rule as shown. Note that "foo" can be a string or a
 * {@link PropertyName}.
 *
 * <pre>
 * &#64;Rule
 * public ZiggyPropertyRule fooPropertyRule = new ZiggyPropertyRule("foo", "value");
 * </pre>
 *
 * If one of the constructors that takes a {@link ZiggyDirectoryRule} is used, drop the
 * {@code @Rule} on the property rule as well as the directory rule (to prevent them from being run
 * twice) and add a chain to ensure that the directory rule is executed first. Additional property
 * rules can added to the chain. For example:
 *
 * <pre>
 * public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();
 * public ZiggyPropertyRule fooPropertyRule = new ZiggyPropertyRule("foo", directoryRule, "bar");
 *
 * &#64;Rule
 * public final RuleChain ruleChain = RuleChain.outerRule(directoryRule).around(fooPropertyRule);
 * </pre>
 *
 * This rule provides a {@link #getValue} method to access the current property value and a
 * {@link #setValue} method to set it. The latter can be used to override the rule's initial value
 * or a system property. Ziggy code should not call either {@link System#getProperty()} or
 * {@link System#setProperty()}.
 * <p>
 * This class is thread-safe as it uses thread-safe configuration objects.
 *
 * @author Bill Wohler
 */
public class ZiggyPropertyRule extends ExternalResource {

    private String property;
    private String value;
    private ZiggyDirectoryRule directoryRule;
    private String subdirectory;

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and value. See class
     * documentation for usage.
     *
     * @param property the non-{@code null} property to set
     * @param value the value to set the property to. This can be {@code null} to clear the property
     * before each test, and reset the property after each test. This is useful if a test modifies
     * the property or depends on the property being cleared.
     * @throws NullPointerException if property is {@code null}
     */
    public ZiggyPropertyRule(PropertyName property, String value) {
        this(checkNotNull(property, "property").property(), value);
    }

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and value. See class
     * documentation for usage.
     *
     * @param property the non-{@code null} property to set
     * @param value the value to set the property to. This can be {@code null} to clear the property
     * before each test, and reset the property after each test. This is useful if a test modifies
     * the property or depends on the property being cleared.
     * @throws NullPointerException if property is {@code null}
     */
    public ZiggyPropertyRule(String property, String value) {
        this.property = checkNotNull(property, "property");
        this.value = value;
    }

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and directory. See class
     * documentation for usage.
     *
     * @param property the non-{@code null} property to set
     * @param directoryRule the non-{@code null} directory rule from which the directory is obtained
     * and used as the value of the property
     * @throws NullPointerException if property or directoryRule are {@code null}
     */
    public ZiggyPropertyRule(PropertyName property, ZiggyDirectoryRule directoryRule) {
        this(property, directoryRule, null);
    }

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and directory. See class
     * documentation for usage.
     *
     * @param property the non-{@code null} property to set
     * @param directoryRule the non-{@code null} directory rule from which the directory is obtained
     * and used as the value of the property
     * @throws NullPointerException if property or directoryRule are {@code null}
     */
    public ZiggyPropertyRule(String property, ZiggyDirectoryRule directoryRule) {
        this(property, directoryRule, null);
    }

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and directory and subdirectory.
     * See class documentation for usage.
     *
     * @param property the non-{@code null} property to set
     * @param directoryRule the non-{@code null} directory rule from which the directory is obtained
     * and used as the value of the property
     * @param subdirectory the subdirectory, which is appended to the directory and used as the
     * value of the property; ignored if {@code null}
     * @throws NullPointerException if property or directoryRule are {@code null}
     */
    public ZiggyPropertyRule(PropertyName property, ZiggyDirectoryRule directoryRule,
        String subdirectory) {
        this(checkNotNull(property, "property").property(), directoryRule, subdirectory);
    }

    /**
     * Creates a {@code ZiggyPropertyRule} with the given property and directory and subdirectory.
     * See class documentation for usage.
     *
     * @param property the non-{@code null} property to set
     * @param directoryRule the non-{@code null} directory rule from which the directory is obtained
     * and used as the value of the property
     * @param subdirectory the subdirectory, which is appended to the directory and used as the
     * value of the property; ignored if {@code null}
     * @throws NullPointerException if property or directoryRule are {@code null}
     */
    public ZiggyPropertyRule(String property, ZiggyDirectoryRule directoryRule,
        String subdirectory) {
        this.property = checkNotNull(property, "property");
        this.directoryRule = checkNotNull(directoryRule, "directoryRule");
        this.subdirectory = subdirectory;
    }

    @Override
    protected void before() throws Throwable {
        if (directoryRule != null) {
            Path directory = directoryRule.directory();
            if (subdirectory != null) {
                directory = directory.resolve(subdirectory);
            }
            value = directory.toString();
        }

        CompositeConfiguration seedConfig = ZiggyConfiguration.getMutableInstance();

        if (seedConfig == null) {
            seedConfig = new CompositeConfiguration();
            ZiggyConfiguration.setMutableInstance(seedConfig);
        }
        setValue(value);
    }

    @Override
    protected void after() {
        ZiggyConfiguration.reset();
    }

    /**
     * Returns the value of the property set by this rule.
     */
    public String getValue() {
        return value;
    }

    /**
     * Updates the initial value of the property. This needs to be called at the beginning of the
     * test method in order to affect future {@link ZiggyConfiguration#getInstance()} calls.
     */
    public void setValue(String value) {
        this.value = value;
        if (value != null) {
            ZiggyConfiguration.getMutableInstance().setProperty(property, value);
        } else {
            ZiggyConfiguration.getMutableInstance().clearProperty(property);
        }
    }
}
