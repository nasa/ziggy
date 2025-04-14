package gov.nasa.ziggy.services.config;

import static org.junit.Assert.assertEquals;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.junit.Rule;
import org.junit.Test;

import gov.nasa.ziggy.ZiggyPropertyRule;

/** Unit tests for {@link PropertyName} class. */
public class PropertyNameTest {

    @Rule
    public ZiggyPropertyRule groupPropertyRule = new ZiggyPropertyRule("ziggy.remote.hecc.group",
        "g1");

    @Rule
    public ZiggyPropertyRule userPropertyRule = new ZiggyPropertyRule("ziggy.remote.gcp.user",
        "u1");

    @Test
    public void testPropertiesRetrieval() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        assertEquals("u1", config.getString(PropertyName.remoteUser("GCP")));
        assertEquals("g1", config.getString(PropertyName.remoteGroup("HECC")));
    }

    @Test
    public void testEmptyPropertiesRetrieval() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        assertEquals("", config.getString(PropertyName.remoteUser("mock"), ""));
        assertEquals("", config.getString(PropertyName.remoteGroup("mock"), ""));
    }
}
