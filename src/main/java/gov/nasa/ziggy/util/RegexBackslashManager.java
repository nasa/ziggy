package gov.nasa.ziggy.util;

import java.util.regex.Matcher;

import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * Handles backslashes in regular expressions.
 * <p>
 * This class takes care of two annoying issues in the way that regular expressions handle the
 * backslash character:
 * <ol>
 * <li>When reading from XML, the double-backslash ("\\") is treated literally, rather than doing
 * what one expects, which is that the double backslash is translated to a single backslash in the
 * resulting Java string.
 * <li>When using the {@link Matcher#appendReplacement(StringBuilder, String)} method, the opposite
 * problem occurs: backslashes in the replacement string need to be "double escaped" (i.e., you need
 * 4 backslashes in the string to get 1 in the output).
 * </ol>
 *
 * @author PT
 */
public class RegexBackslashManager {

    public static String toSingleBackslash(String stringWithDoubleBackslash) {
        return stringWithDoubleBackslash.replace("\\\\", "\\");
    }

    public static String toDoubleBackslash(String stringWithSingleBackslash) {
        return stringWithSingleBackslash.replace("\\", "\\\\");
    }

    /**
     * Class that provides automatic conversion of regex strings in XML.
     */
    public static class XmlRegexAdapter extends XmlAdapter<String, String> {

        @Override
        public String unmarshal(String xmlString) {
            return toSingleBackslash(xmlString);
        }

        @Override
        public String marshal(String javaString) {
            return toDoubleBackslash(javaString);
        }
    }
}
