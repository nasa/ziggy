package gov.nasa.ziggy.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Regular expression processing of text and text files.
 *
 * @author Forrest Girouard
 */
public class RegexEditor {
    public interface FindAction {
        /**
         * Updates the current line that has matched a pattern.
         *
         * @param matcher the Matcher object for the current line
         * @return the updated line, or {@code null} to delete the line
         */
        String update(Matcher matcher);
    }

    public static class ExtractCaptureGroups implements FindAction {
        @Override
        public String update(Matcher matcher) {
            StringBuilder builder = new StringBuilder();
            for (int group = 1; group <= matcher.groupCount(); group++) {
                String match = matcher.group(group);
                if (match != null) {
                    builder.append(match);
                }
            }
            return builder.toString();
        }
    }

    public static String findAndReplaceText(String input, Pattern pattern) {
        return findAndReplaceText(input, pattern, new ExtractCaptureGroups());
    }

    public static String findAndReplaceText(String input, Pattern[] patterns) {
        String output = input;
        if (patterns != null) {
            for (Pattern pattern : patterns) {
                output = findAndReplaceText(output, pattern, new ExtractCaptureGroups());
            }
        }
        return output;
    }

    public static String findAndReplaceText(String input, Pattern pattern, FindAction action) {
        if (action == null) {
            throw new NullPointerException("null action");
        }

        String output = input;
        if (pattern != null) {
            Matcher matcher = pattern.matcher(input);
            if (matcher.find()) {
                output = action.update(matcher);
            }
        }
        return output;
    }

    public static File findAndReplace(File file, Pattern pattern) throws IOException {
        return findAndReplace(file, pattern, null);
    }

    public static File findAndReplace(File file, Pattern pattern, File directory)
        throws IOException {
        return findAndReplace(file, pattern, new ExtractCaptureGroups(), directory);
    }

    public static File findAndReplace(File file, Pattern pattern, FindAction action, File directory)
        throws IOException {
        File tmpFile = File.createTempFile(FilenameUtils.getBaseName(file.toString()),
            FilenameUtils.getExtension(file.toString()), directory);
        if (pattern != null) {
            LineIterator lines = FileUtils.lineIterator(file);
            List<String> output = new ArrayList<>();
            if (lines != null) {
                while (lines.hasNext()) {
                    String line = lines.nextLine();
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        line = action.update(matcher);
                    }
                    if (line != null) {
                        output.add(line);
                    }
                }
            }
            FileUtils.writeLines(tmpFile, output);
        } else {
            FileUtils.copyFile(file, tmpFile, true);
        }
        return tmpFile;
    }

    public static File findAndReplaceAll(File sourceDir, Pattern pattern, FindAction action)
        throws IOException {
        File output = null;
        if (pattern != null) {
            Collection<File> files = FileUtils.listFiles(sourceDir, null, false);
            if (files != null) {
                for (File file : files) {
                    output = findAndReplace(file, pattern, action, null);
                }
            }
        } else {
            output = File.createTempFile(FilenameUtils.getBaseName(sourceDir.toString()), null);
            FileUtils.forceMkdir(output);
            FileUtils.copyDirectory(sourceDir, output);
        }
        return output;
    }

    public static File findAndReplace(File sourceDir, IOFileFilter[] filters, Pattern pattern,
        FindAction action) throws IOException {
        File output = null;
        if (filters != null) {
            output = new File(DirectoryProperties.tmpDir().toFile(),
                FilenameUtils.getBaseName(sourceDir.toString()));
            if (output.exists()) {
                FileUtils.forceDelete(output);
            }
            FileUtils.forceMkdir(output);
            for (IOFileFilter filter : filters) {
                Collection<File> files = FileUtils.listFiles(sourceDir, filter,
                    FileFilterUtils.directoryFileFilter());
                if (files != null) {
                    File outputFile = null;
                    File buildTmp = DirectoryProperties.tmpDir().toFile();
                    for (File file : files) {
                        outputFile = findAndReplace(file, pattern, action, buildTmp);
                        FileUtils.copyFile(outputFile, new File(output, file.getName()));
                    }
                }
            }
        } else {
            output = findAndReplaceAll(sourceDir, pattern, action);
        }
        return output;
    }

    public static boolean stringEquals(String expectedInput, String actualInput, Pattern[] ignore) {
        String expected = RegexEditor.findAndReplaceText(expectedInput, ignore);
        String actual = RegexEditor.findAndReplaceText(actualInput, ignore);

        return expected.equals(actual);
    }

    public static String createCompoundRegex(List<String> regexs) {
        if (regexs == null) {
            throw new NullPointerException("regexs is null");
        }
        if (regexs.size() == 0) {
            throw new IllegalArgumentException("regexs is zero length");
        }

        StringBuilder compoundExpression = new StringBuilder();
        for (String regex : regexs) {
            if (compoundExpression.length() > 0) {
                compoundExpression.append("|");
            }
            compoundExpression.append(regex);
        }
        return compoundExpression.toString();
    }

    public static Pattern createCompoundPattern(List<String> regexs) {
        return Pattern.compile(createCompoundRegex(regexs));
    }
}
