<#-- Template for generating ZiggyVersion.java -->
package gov.nasa.ziggy.util;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides code versioning information.
 *
 * <p>This has been automatically generated! Do not edit.
 */
public class ZiggyVersion {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
    
    private static final Pattern NON_TAG_PATTERN = Pattern.compile("^.*-g[A-Fa-f0-9]+$");
    
    /**
     * Gets the build date.
     *
     * @return the date and time this file was created, as a Java date
     * @throws IllegalStateException if there is an error interpreting the build date string
     */
    public static Date getBuildDate() {
        try {
            return DATE_FORMAT.parse("${buildDate}");
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }
    }
     
    /**
     * Gets the software revision. The format will vary by revision control
     * system. For Git repositories, this should be generated by "git describe".
     *
     * @return the software version, as a string
     */
    public static String getSoftwareVersion() {
        return "${softwareVersion}";
    }
    
    /**
     * Gets the latest version control commit revision identifier. For Git repositories, this
     * is the commit hash.
     *
     * @return the latest commit revision identifier, as a string
     */
    public static String getRevision() {
        return "${revision}";
    }

    /**
     * Tests whether the software revision corresponds to a release. In TESS,
     * we are running a release if it was compiled from a release branch.
     *     
     * @return true, if the software revision is a released version
     */
    public static boolean isRelease() {
        return getBranch().startsWith("releases/") || (atTag() && getBranch().equals("HEAD"));
    }
     
    /**
     * Gets the branch of the software revision.
     *
     * @return the branch used to build this file
     */
    public static String getBranch() {
         return "${branch}";
    }
    
    /**
     * Tests whether we have been checked out from a tag. If so, we
     * will be in a detached head state, in which case the software
     * revision obtained by "git describe" will not have a trailing
     * "{@code -g<commit hash>}".
     *
     * @return true, if we have been checked out from a tag, false otherwise
     */
    private static boolean atTag() {
        Matcher matcher = NON_TAG_PATTERN.matcher(getSoftwareVersion());
        return !matcher.matches();
    }
    
} 
