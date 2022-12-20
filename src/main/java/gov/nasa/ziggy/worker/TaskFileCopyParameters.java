package gov.nasa.ziggy.worker;

import gov.nasa.ziggy.parameters.Parameters;

/**
 * Parameters for copying task files from the worker to another directory, typically a shared
 * volume. These parameters are used by {@link TaskFileCopy}
 *
 * @author Todd Klaus
 */
public class TaskFileCopyParameters implements Parameters {
    private boolean enabled = false;

    /**
     * Absolute path to the destination directory.
     */
    private String destinationPath;

    /**
     * Files with any of the specified suffixes will be excluded from the copy. Wildcards should not
     * be used ('.txt' not '*.txt').
     */
    private String[] excludeWildcards = {};

    /**
     * If true, an exception is thrown if the copy fails
     */
    private boolean failTaskOnError = false;

    /**
     * Delete the source after the copy is complete (without errors).
     */
    private boolean deleteAfterCopy = false;

    /**
     * Delete the source without copying. This option destroys the task files so it should only be
     * used in testing scenarios where that is acceptable
     */
    private boolean deleteWithoutCopy = false;

    /**
     * Directory where the symlinks will be created.
     */
    private String uowSymlinkPath;

    /**
     * Specifies whether to include month name in UOW string
     */
    private boolean uowSymlinksIncludeMonths = false;

    /**
     * Specifies whether to include cadence ranges in the symlink names.
     */
    private boolean isUowSymlinksIncludeCadenceRange;

    public TaskFileCopyParameters() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

    public void setDestinationPath(String destinationPath) {
        this.destinationPath = destinationPath;
    }

    public String[] getExcludeWildcards() {
        return excludeWildcards;
    }

    public void setExcludeWildcards(String[] excludeWildcards) {
        this.excludeWildcards = excludeWildcards;
    }

    public boolean isFailTaskOnError() {
        return failTaskOnError;
    }

    public void setFailTaskOnError(boolean failTaskOnError) {
        this.failTaskOnError = failTaskOnError;
    }

    public boolean isDeleteAfterCopy() {
        return deleteAfterCopy;
    }

    public void setDeleteAfterCopy(boolean deleteAfterCopy) {
        this.deleteAfterCopy = deleteAfterCopy;
    }

    public boolean isDeleteWithoutCopy() {
        return deleteWithoutCopy;
    }

    public void setDeleteWithoutCopy(boolean deleteWithoutCopy) {
        this.deleteWithoutCopy = deleteWithoutCopy;
    }

    public String getUowSymlinkPath() {
        return uowSymlinkPath;
    }

    public void setUowSymlinkPath(String uowSymlinkPath) {
        this.uowSymlinkPath = uowSymlinkPath;
    }

    public boolean isUowSymlinksIncludeMonths() {
        return uowSymlinksIncludeMonths;
    }

    public void setUowSymlinksIncludeMonths(boolean uowSymlinksIncludeMonths) {
        this.uowSymlinksIncludeMonths = uowSymlinksIncludeMonths;
    }

    public boolean isUowSymlinksIncludeCadenceRange() {
        return isUowSymlinksIncludeCadenceRange;
    }

    public void setUowSymlinksIncludeCadenceRange(boolean isUowSymlinksIncludeCadenceRange) {
        this.isUowSymlinksIncludeCadenceRange = isUowSymlinksIncludeCadenceRange;
    }

}
