package gov.nasa.ziggy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.pipeline.xml.XmlReference;

/**
 * Utility methods for use in unit tests of the pipeline definition XML infrastructure.
 *
 * @author PT
 */
public class XmlUtils {

    /**
     * Returns the sublist of a {@link List} of {@link String}s that contains the definition of a
     * pipeline node. The node is located by searching for a specified {@link String} of content
     * within the {@link List}.
     */
    public static List<String> nodeContent(List<String> allContents, String nodeDefinitionLine) {
        return xmlElementContent(allContents, nodeDefinitionLine, "</node>");
    }

    /**
     * Returns the sublist of a {@link List} of {@link String}s that contains the definition of a
     * pipeline. The pipeline is located by searching for a specified {@link String} of content
     * within the {@link List}.
     */
    public static List<String> pipelineContent(List<String> allContents,
        String pipelineDefinitionLine) {
        return xmlElementContent(allContents, pipelineDefinitionLine, "</pipeline>");
    }

    /**
     * Returns the sublist of a {@link List} of {@link String}s that corresponds to a specified XML
     * element definition. The XML element is located by finding the start and end content specified
     * as {@link String} arguments. The method also asserts that the returned {@link List} is not
     * empty.
     */
    private static List<String> xmlElementContent(List<String> allContents,
        String xmlDefinitionLine, String elementEndText) {
        List<String> xmlElementContents = new ArrayList<>();
        int elementStartLine = -1;
        for (String content : allContents) {
            if (content.contains(xmlDefinitionLine)) {
                elementStartLine = allContents.indexOf(content);
                break;
            }
        }
        if (elementStartLine >= 0) {
            for (int elemCount = elementStartLine; elemCount <= allContents.size(); elemCount++) {
                xmlElementContents.add(allContents.get(elemCount));
                if (allContents.get(elemCount).contains(elementEndText)) {
                    break;
                }
            }
        }
        assertFalse(xmlElementContents.isEmpty());
        assertContains(xmlElementContents, elementEndText);
        return xmlElementContents;
    }

    /**
     * Asserts that a {@link List} of {@link String}s contains a {@link String} that, in turn,
     * contains specified content. The assertion will be true if there is at least one
     * {@link String} in the {@link List} such that, when the {@link String#trim()} operator is
     * applied to the {@link String}, it is identical to the specified content. That is to say, the
     * method operates analogously to the {@link List#contains(Object)} method except that the
     * object of the {@link List#contains(Object)} does not need to be exactly matched; a match that
     * differs only by leading or trailing whitespace will be accepted.
     */
    public static void assertContains(List<String> contents, String contentToFind) {
        boolean contains = false;
        for (String content : contents) {
            if (content.trim().equals(contentToFind.trim())) {
                contains = true;
                break;
            }
        }
        assertTrue(contentToFind, contains);
    }

    public static boolean containsAllElements(List<String> xmlStringElements,
        List<String> lineToFindElements) {
        return xmlStringElements.containsAll(lineToFindElements);
    }

    /**
     * Compares two {@link Collection}s of {@link XmlReference} instances. The comparison asserts
     * that the two have the same length, and that each reference in one {@link Collection} is
     * present in the other.
     */
    public static void compareXmlReferences(
        Collection<? extends XmlReference> groundTruthReferences,
        Collection<? extends XmlReference> nodeReferences) {
        assertEquals(groundTruthReferences.size(), nodeReferences.size());
        for (XmlReference reference : nodeReferences) {
            assertTrue(groundTruthReferences.contains(reference));
        }
    }

    /**
     * Compares two {@link Collection}s of {@link ParameterSetName} instances. The comparison
     * asserts that the two have the same length, and that each name in one {@link Collection} is
     * present in the other.
     */
    public static void compareParameterSetReferences(Collection<String> groundTruthReferences,
        Collection<String> nodeReferences) {
        assertEquals(groundTruthReferences.size(), nodeReferences.size());
        for (String reference : nodeReferences) {
            assertTrue(groundTruthReferences.contains(reference));
        }
    }

    /**
     * Returns the sublist of a {@link List} of {@link String}s that contains the definition of a
     * complex type in an XML schema. The element is located by searching for a specified
     * {@link String} of content within the {@link List}.
     */
    public static List<String> complexTypeContent(List<String> allContents,
        String pipelineDefinitionLine) {
        return xmlElementContent(allContents, pipelineDefinitionLine, "</xs:complexType>");
    }

    /**
     * Returns the sublist of a {@link List} of {@link String}s that contains the definition of a
     * simple type in an XML schema. The element is located by searching for a specified
     * {@link String} of content within the {@link List}.
     */
    public static List<String> simpleTypeContent(List<String> allContents,
        String pipelineDefinitionLine) {
        return xmlElementContent(allContents, pipelineDefinitionLine, "</xs:simpleType>");
    }
}
