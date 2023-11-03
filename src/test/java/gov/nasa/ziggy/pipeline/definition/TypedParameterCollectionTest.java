package gov.nasa.ziggy.pipeline.definition;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the TypedParameterCollection class.
 *
 * @author Bill Wohler
 */
public class TypedParameterCollectionTest {

    private TypedParameterCollection collection1;
    private TypedParameterCollection collection2;
    private TypedParameter a;
    private TypedParameter b;
    private TypedParameter c;

    @Before
    public void setup() {
        // Collection1 initialized with add().
        collection1 = new TypedParameterCollection();
        a = new TypedParameter("a", " a , a ");
        collection1.addParameter(a);
        b = new TypedParameter("b", " b,b ");
        collection1.addParameter(b);
        c = new TypedParameter("c", " xy zzy ");
        collection1.addParameter(c);

        // Collection2 initialized with setParameters().
        collection2 = new TypedParameterCollection();
        collection2.setParameters(Set.of(a, b, c));
    }

    // Test that the value's whitespace is trimmed.
    @Test
    public void testTrimWhitespace() {
        assertEquals("a,a", a.getValue());
        assertEquals("b,b", b.getValue());
        assertEquals("xy zzy", c.getValue());
    }

    @Test
    public void testOriginalsGetModifiedWithAdd() {
        testOriginalsGetModified(collection1);
    }

    @Test
    public void testOriginalsGetModifiedWithSetParameters() {
        testOriginalsGetModified(collection2);
    }

    private void testOriginalsGetModified(TypedParameterCollection collection) {
        // Changes to the original objects should affect the collection we created.
        a.setValue("new value");
        assertEquals("new value", collection.getParameter("a").getValue());

        // Changes to the objects retrieved from the collection should also affect the
        // original objects.
        collection1.getParameter("b").setValue("new value");
        assertEquals("new value", b.getValue());
    }

    @Test
    public void testOriginalsGetModifiedWithGetParameters() {
        // Changes to the original objects should affect the collection we created.
        a.setValue("new value");
        for (TypedParameter parameter : collection1.getParameters()) {
            if (parameter.getName() == "a") {
                assertEquals("new value", parameter.getValue());
            }
        }

        // Changes to the objects retrieved from the collection should also affect the
        // original objects.
        for (TypedParameter parameter : collection1.getParameters()) {
            if (parameter.getName() == "b") {
                parameter.setValue("new value");
            }
        }
        assertEquals("new value", b.getValue());
    }

    @Test
    public void testOriginalsGetModifiedWithGetParametersByName() {
        // Changes to the original objects should affect the collection we created.
        a.setValue("new value");
        for (TypedParameter parameter : collection1.getParameters()) {
            if (parameter.getName() == "a") {
                assertEquals("new value", parameter.getValue());
            }
        }

        // Changes to the objects retrieved from the collection should also affect the
        // original objects.
        for (TypedParameter parameter : collection1.getParameters()) {
            if (parameter.getName() == "b") {
                parameter.setValue("new value");
            }
        }
        assertEquals("new value", b.getValue());
    }
}
