package gov.nasa.ziggy.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.RegexEditor.FindAction;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

public class RegexEditorTest {
    private static final String SIMPLE_REGEX = "^([0-9]*)(?:[^0-9]*)([0-9]*)(?:.*)$";
    private static final String SIMPLE_TEXT = "29 April 2008";
    private static final String FC_TABLE_REGEX = "^(CREATE MEMORY TABLE )(FC_)([^\\(]*)(.*)$";
    private static final String FC_TABLE_TEXT = """
        CREATE MEMORY TABLE FC_2DBLACK_IMAGE\
        (ID BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1) NOT NULL PRIMARY KEY,\
        FC_IMAGE_UNCERTAINTY_ID BIGINT,FC_IMAGE_DATA_ID BIGINT)""";

    @Test(expected = NullPointerException.class)
    public void createCompoundRegexNull() {
        RegexEditor.createCompoundRegex(null);
        fail("null regexs parameter");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCompoundRegexEmpty() {
        List<String> regexs = new ArrayList<>();
        RegexEditor.createCompoundRegex(regexs);
        fail("empty regexs parameter");
    }

    @Test
    public void createCompoundRegexSingle() {
        List<String> regexs = new ArrayList<>();
        regexs.add(SIMPLE_REGEX);
        assertEquals("single regex", SIMPLE_REGEX, RegexEditor.createCompoundRegex(regexs));
    }

    @Test
    public void createCompoundRegexMultiple() {
        List<String> regexs = new ArrayList<>();
        regexs.add(SIMPLE_REGEX);
        regexs.add(SIMPLE_REGEX);
        assertEquals("multiple regexs", SIMPLE_REGEX + "|" + SIMPLE_REGEX,
            RegexEditor.createCompoundRegex(regexs));
    }

    @Test(expected = NullPointerException.class)
    public void createCompoundPatternNull() {
        RegexEditor.createCompoundPattern(null);
        fail("null regexs parameter");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createCompoundPatternEmpty() {
        List<String> regexs = new ArrayList<>();
        RegexEditor.createCompoundPattern(regexs);
        fail("empty regexs parameter");
    }

    @Test
    public void createCompoundPatternSingle() {
        List<String> regexs = new ArrayList<>();
        regexs.add(SIMPLE_REGEX);
        assertEquals("single regex", Pattern.compile(SIMPLE_REGEX).pattern(),
            RegexEditor.createCompoundPattern(regexs).pattern());
    }

    @Test
    public void createCompoundPatternMultiple() {
        List<String> regexs = new ArrayList<>();
        regexs.add(SIMPLE_REGEX);
        regexs.add(SIMPLE_REGEX);
        assertEquals("multiple regexs",
            Pattern.compile(SIMPLE_REGEX + "|" + SIMPLE_REGEX).pattern(),
            RegexEditor.createCompoundPattern(regexs).pattern());
    }

    @Test
    public void findAndReplaceTextSimple() {
        assertEquals("text", "292008",
            RegexEditor.findAndReplaceText(SIMPLE_TEXT, Pattern.compile(SIMPLE_REGEX)));
    }

    @Test
    public void findAndReplaceTextReverse() {
        assertEquals("reverse text", "200829", RegexEditor.findAndReplaceText(SIMPLE_TEXT,
            Pattern.compile(SIMPLE_REGEX), new ReverseGroups()));
    }

    @Test
    public void findAndReplace() throws IOException {
        File tmpFile = null;
        Writer output = null;
        BufferedReader input = null;
        try {
            tmpFile = File.createTempFile(RegexEditor.class.getSimpleName(), "txt");
            output = new BufferedWriter(new FileWriter(tmpFile, ZiggyFileUtils.ZIGGY_CHARSET));
            output.write(SIMPLE_TEXT);
            output.close();
            output = null;

            File editedFile = RegexEditor.findAndReplace(tmpFile, Pattern.compile(SIMPLE_REGEX));
            input = new BufferedReader(new FileReader(editedFile, ZiggyFileUtils.ZIGGY_CHARSET));
            assertEquals("text file", "292008", input.readLine());
        } finally {
            ZiggyFileUtils.close(output);
            ZiggyFileUtils.close(input);
            if (tmpFile != null) {
                tmpFile.delete();
            }
        }
    }

    @Test
    public void findAndReplaceReverse() throws IOException {
        File tmpFile = null;
        File buildTmp = DirectoryProperties.tmpDir().toFile();
        Writer output = null;
        BufferedReader input = null;
        try {
            tmpFile = File.createTempFile(RegexEditor.class.getSimpleName(), "txt", buildTmp);
            output = new BufferedWriter(new FileWriter(tmpFile, ZiggyFileUtils.ZIGGY_CHARSET));
            output.write(SIMPLE_TEXT);
            output.close();
            output = null;

            File editedFile = RegexEditor.findAndReplace(tmpFile, Pattern.compile(SIMPLE_REGEX),
                new ReverseGroups(), buildTmp);
            input = new BufferedReader(new FileReader(editedFile, ZiggyFileUtils.ZIGGY_CHARSET));
            assertEquals("text file", "200829", input.readLine());
        } finally {
            ZiggyFileUtils.close(output);
            ZiggyFileUtils.close(input);
            if (tmpFile != null) {
                tmpFile.delete();
            }
        }
    }

    @Test
    public void findAndUpdate() throws IOException {
        File tmpFile = null;
        File buildTmp = DirectoryProperties.tmpDir().toFile();
        Writer output = null;
        BufferedReader input = null;
        try {
            tmpFile = File.createTempFile(RegexEditor.class.getSimpleName(), "txt", buildTmp);
            output = new BufferedWriter(new FileWriter(tmpFile, ZiggyFileUtils.ZIGGY_CHARSET));
            output.write(FC_TABLE_TEXT);
            output.close();
            output = null;

            File editedFile = RegexEditor.findAndReplace(tmpFile, Pattern.compile(FC_TABLE_REGEX),
                new CaptureGroups("CREATE CACHED TABLE "), buildTmp);
            input = new BufferedReader(new FileReader(editedFile, ZiggyFileUtils.ZIGGY_CHARSET));
            assertTrue("create cached table",
                input.readLine().startsWith("CREATE CACHED TABLE FC_"));
        } finally {
            ZiggyFileUtils.close(output);
            ZiggyFileUtils.close(input);
            if (tmpFile != null) {
                tmpFile.delete();
            }
        }
    }

    public static class ReverseGroups implements FindAction {
        @Override
        public String update(Matcher matcher) {
            StringBuilder builder = new StringBuilder();
            for (int group = matcher.groupCount(); group > 0; group--) {
                builder.append(matcher.group(group));
            }
            return builder.toString();
        }
    }

    /**
     * A find action to performed on text matching a regular expression that replaces the matched
     * input with the capture groups.
     *
     * @author Forrest Girouard
     */
    public static class CaptureGroups implements FindAction {
        private final String prefix;

        public CaptureGroups(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String update(Matcher matcher) {
            if (matcher.groupCount() < 2) {
                throw new IllegalStateException(
                    matcher.groupCount() + ": expected at least two capture groups");
            }
            StringBuilder builder = new StringBuilder(prefix);
            for (int group = 2; group <= matcher.groupCount(); group++) {
                builder.append(matcher.group(group));
            }
            return builder.toString();
        }
    }
}
