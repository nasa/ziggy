package gov.nasa.ziggy;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import gov.nasa.ziggy.services.config.PropertyName;

/**
 * All of our tests test the constructors that take a {@link PropertyName}. What they don't test are
 * the constructors that take a string, which may be used by other projects. Test those here.
 */
public class ZiggyPropertyRuleTest {
    public ZiggyDirectoryRule directoryRule = new ZiggyDirectoryRule();

    @Rule
    public ZiggyPropertyRule stringPropertyRule = new ZiggyPropertyRule("string", "value");

    public ZiggyPropertyRule stringDirectoryPropertyRule = new ZiggyPropertyRule("directory-string",
        directoryRule);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(directoryRule)
        .around(stringDirectoryPropertyRule);

    @Test
    public void stringConstructorTest() {
        assertEquals("value", stringPropertyRule.getValue());
        assertEquals("build/test/ZiggyPropertyRuleTest/stringConstructorTest",
            stringDirectoryPropertyRule.getValue());
    }
}
