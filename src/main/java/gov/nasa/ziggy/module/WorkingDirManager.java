package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Names, creates, and deletes the temporary working directories for external process invocation.
 *
 * @author Todd Klaus
 */
public class WorkingDirManager {
    private static final Logger log = LoggerFactory.getLogger(WorkingDirManager.class);

    private final File rootWorkingDir;

    public WorkingDirManager() {
        rootWorkingDir = new File(workingDirParent());
    }

    public static String workingDirParent() {

        return DirectoryProperties.taskDataDir().toString();
    }

    public static File workingDirBaseName(PipelineTask pipelineTask) {
        return new File(pipelineTask.taskBaseName());
    }

    public static File workingDir(PipelineTask pipelineTask) {
        return workingDir(pipelineTask.taskBaseName());
    }

    private static File workingDir(String taskBaseName) {
        return new File(workingDirParent(), taskBaseName);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public synchronized File allocateWorkingDir(PipelineTask pipelineTask, boolean cleanExisting) {
        File workingDir = new File(rootWorkingDir, pipelineTask.taskBaseName());

        if (workingDir.exists() && cleanExisting) {
            log.info(
                "Working directory for name=" + pipelineTask.getId() + " already exists, deleting");
            FileUtil.deleteDirectoryTree(workingDir.toPath());
        }

        log.info("Creating task working dir: " + workingDir);
        try {
            FileUtils.forceMkdir(workingDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create dir " + workingDir.toString(), e);
        }

        return workingDir;
    }
}
