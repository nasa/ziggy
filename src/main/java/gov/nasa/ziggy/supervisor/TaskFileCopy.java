package gov.nasa.ziggy.supervisor;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;

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
import gov.nasa.ziggy.module.TaskDirectoryManager;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Copy task files from the task data directory to another directory, typically a shared volume.
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

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public void copyTaskFiles() {
        File srcTaskDir = new TaskDirectoryManager(pipelineTask).taskDir().toFile();
        try {
            if (copyParams.isDeleteWithoutCopy()) {
                log.warn("*** TEST USE ONLY ***: deleting source directory without copying");
                FileUtils.forceDelete(srcTaskDir);
                log.info("Done deleting source directory");
                return;
            }
            String destPath = copyParams.getDestinationPath();
            File destDir = new File(destPath);

            if (!destDir.exists()) {
                FileUtils.forceMkdir(destDir);
            }

            log.info("srcTaskDir = " + srcTaskDir);
            log.info("destPath = " + destPath);

            if (!destDir.exists() || !destDir.isDirectory()) {
                throw new PipelineException(
                    "destDir does not exist or is not a directory: " + destPath);
            }

            if (srcTaskDir == null) {
                throw new PipelineException(
                    "sourceTaskDir not found for pipelineTask: " + pipelineTask);
            }

            File destTaskDir = new File(destDir, srcTaskDir.getName());
            File destTaskTmpDir = new File(destDir, srcTaskDir.getName() + ".in_progress");

            log.info("destTaskDir = " + destTaskDir);

            if (destTaskTmpDir.exists()) {
                log.info("destTaskTmpDir already exists, deleting: " + destTaskTmpDir);
                FileUtils.forceDelete(destTaskTmpDir);
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
            if (excludeFilter != null) {
                FileUtils.copyDirectory(srcTaskDir, destTaskTmpDir, excludeFilter);
            } else {
                FileUtils.copyDirectory(srcTaskDir, destTaskTmpDir);
            }

            // rename dest now that copy is complete
            boolean renamed = destTaskTmpDir.renameTo(destTaskDir);
            if (!renamed) {
                throw new PipelineException(
                    "Failed to rename: " + destTaskTmpDir + " -> " + destTaskDir);
            }

            // delete source
            if (copyParams.isDeleteAfterCopy()) {
                log.info("Copy complete, deleting source directory");
                FileUtils.forceDelete(srcTaskDir);
                log.info("Done deleting source directory");
            }

            // attempt to measure dest file sizes
            long archiveSize = FileUtils.sizeOfDirectory(destTaskDir);
            ValueMetric.addValue(PipelineMetrics.TF_ARCHIVE_SIZE_METRIC, archiveSize);
        } catch (IOException e) {
            throw new UncheckedIOException("IOException occurred when copying task files ", e);
        }
    }
}
