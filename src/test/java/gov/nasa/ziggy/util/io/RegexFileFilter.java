package gov.nasa.ziggy.util.io;

import java.io.File;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.AbstractFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Matches expression(s) against the full path name of each file. So if you want to just match the
 * file name then you need to prefix the regex with dot star forward slash. Can't actually put that
 * in this comment because it would terminate the comment.
 *
 * @author Forrest Girouard
 */
public class RegexFileFilter extends AbstractFileFilter {
    private final Pattern pattern;
    private final IOFileFilter directoryFilter;

    public RegexFileFilter(String expression) {
        this(new String[] { expression }, null);
    }

    public RegexFileFilter(String[] expressions) {
        this(expressions, null);
    }

    public RegexFileFilter(String expression, IOFileFilter directoryFilter) {
        this(new String[] { expression }, directoryFilter);
    }

    public RegexFileFilter(String[] expressions, IOFileFilter directoryFilter) {
        if (expressions == null) {
            throw new NullPointerException("expressions is null");
        }
        if (expressions.length == 0) {
            throw new IllegalArgumentException("expressions is empty");
        }
        this.directoryFilter = directoryFilter;
        String buffer = String.join("|", expressions);
        pattern = Pattern.compile(buffer);
    }

    @Override
    public boolean accept(File file) {
        if (file.isDirectory() && directoryFilter != null) {
            return directoryFilter.accept(file);
        }

        String path = file.getAbsolutePath();
        if (pattern.matcher(path).matches()) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
