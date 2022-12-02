package gov.nasa.ziggy.worker;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.ValueMetric;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.module.PipelineMetrics;
import gov.nasa.ziggy.module.WorkingDirManager;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Copy task files from the worker to another directory, typically a shared volume.
 * <p>
 * This class uses {@link TaskFileCopyParameters} to control behavior.
 *
 * @author Todd Klaus
 */
public class TaskFileCopy {
    private static final Logger log = LoggerFactory.getLogger(TaskFileCopy.class);

    private final PipelineTask pipelineTask;
    private final TaskFileCopyParameters copyParams;

    public TaskFileCopy(PipelineTask pipelineTask, TaskFileCopyParameters copyParams) {
        this.pipelineTask = pipelineTask;
        this.copyParams = copyParams;
    }

    public void copyTaskFiles() throws Exception {
        File srcTaskDir = WorkingDirManager.workingDir(pipelineTask);

        if (copyParams.isDeleteWithoutCopy()) {
            log.warn("*** TEST USE ONLY ***: deleting source directory without copying");
            try {
                FileUtils.forceDelete(srcTaskDir);
            } catch (IOException e) {
                handleError("Failed to delete source task dir: " + srcTaskDir + ", caught e = " + e,
                    e);
                return;
            }
            log.info("Done deleting source directory");
        } else {
            String destPath = copyParams.getDestinationPath();
            File destDir = new File(destPath);

            if (!destDir.exists()) {
                try {
                    FileUtils.forceMkdir(destDir);
                } catch (IOException e1) {
                    handleError("Unable to create destDir: " + destDir + ", caught e=" + e1);
                    return;
                }
            }

            log.info("srcTaskDir = " + srcTaskDir);
            log.info("destPath = " + destPath);

            if (!destDir.exists() || !destDir.isDirectory()) {
                handleError("destDir does not exist or is not a directory: " + destPath);
                return;
            }

            if (srcTaskDir != null) {
                File destTaskDir = new File(destDir, srcTaskDir.getName());
                File destTaskTmpDir = new File(destDir, srcTaskDir.getName() + ".in_progress");

                log.info("destTaskDir = " + destTaskDir);

                if (destTaskTmpDir.exists()) {
                    try {
                        log.info("destTaskTmpDir already exists, deleting: " + destTaskTmpDir);
                        FileUtils.forceDelete(destTaskTmpDir);
                    } catch (IOException e) {
                        handleError("Unable to delete existing destTaskTmpDir: " + destTaskTmpDir
                            + ", caught e=" + e);
                        return;
                    }
                }

                // setup exclude filter
                FileFilter excludeFilter = null;
                String[] wildcards = copyParams.getExcludeWildcards();
                if (wildcards != null && wildcards.length > 0) {
                    excludeFilter = new NotFileFilter(new WildcardFileFilter(wildcards));
                    NotFileFilter svnFilter = new NotFileFilter(new NameFileFilter(".svn"));
                    excludeFilter = new AndFileFilter(svnFilter, (IOFileFilter) excludeFilter);
                    log.info("Using filter: " + excludeFilter);
                }

                // do copy
                try {
                    if (excludeFilter != null) {
                        FileUtils.copyDirectory(srcTaskDir, destTaskTmpDir, excludeFilter);
                    } else {
                        FileUtils.copyDirectory(srcTaskDir, destTaskTmpDir);
                    }
                } catch (IOException e) {
                    handleError("Failed to copy task dir: " + srcTaskDir + ", caught e = " + e, e);
                    return;
                }

                // rename dest now that copy is complete
                boolean renamed = destTaskTmpDir.renameTo(destTaskDir);
                if (!renamed) {
                    handleError("Failed to rename: " + destTaskTmpDir + " -> " + destTaskDir);
                    return;
                }

                // delete source
                if (copyParams.isDeleteAfterCopy()) {
                    log.info("Copy complete, deleting source directory");
                    try {
                        FileUtils.forceDelete(srcTaskDir);
                    } catch (IOException e) {
                        handleError(
                            "Failed to delete source task dir: " + srcTaskDir + ", caught e = " + e,
                            e);
                        return;
                    }
                    log.info("Done deleting source directory");
                }

                // attempt to measure dest file sizes
                try {
                    long archiveSize = FileUtils.sizeOfDirectory(destTaskDir);
                    ValueMetric.addValue(PipelineMetrics.TF_ARCHIVE_SIZE_METRIC, archiveSize);
                } catch (Exception e) {
                    log.warn("Failed to size destTaskDir: " + destTaskDir + ", caught e = " + e, e);
                }
            } else {
                handleError("sourceTaskDir not found for pipelineTask: " + pipelineTask);
            }
        }
    }

    private void handleError(String msg) {
        handleError(msg, null);
    }

    private void handleError(String msg, Exception e) {
        log.error(msg, e);
        if (copyParams.isFailTaskOnError()) {
            throw new PipelineException(msg, e);
        }
    }
}
