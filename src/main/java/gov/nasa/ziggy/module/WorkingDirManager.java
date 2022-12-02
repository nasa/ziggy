package gov.nasa.ziggy.module;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
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

        String workingDirParent = DirectoryProperties.taskDataDir().toString();
        return workingDirParent;
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

    public synchronized File allocateWorkingDir(PipelineTask pipelineTask, boolean cleanExisting)
        throws IOException {
        File workingDir = new File(rootWorkingDir, pipelineTask.taskBaseName());

        if (workingDir.exists() && cleanExisting) {
            log.info(
                "Working directory for name=" + pipelineTask.getId() + " already exists, deleting");
            FileUtil.deleteDirectoryTree(workingDir.toPath());
        }

        log.info("Creating task working dir: " + workingDir);
        FileUtils.forceMkdir(workingDir);

        return workingDir;
    }

}
