package gov.nasa.ziggy.module;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;

/**
 * Names and creates the task directories for external process invocation.
 *
 * @author Todd Klaus
 */
public class TaskDirectoryManager {
    private static final Logger log = LoggerFactory.getLogger(TaskDirectoryManager.class);

    private final Path taskDataDir;
    private final PipelineTask pipelineTask;

    public TaskDirectoryManager(PipelineTask pipelineTask) {
        taskDataDir = DirectoryProperties.taskDataDir();
        this.pipelineTask = pipelineTask;
    }

    public Path taskDir() {
        return taskDir(pipelineTask.taskBaseName());
    }

    private Path taskDir(String taskBaseName) {
        return taskDataDir.resolve(taskBaseName);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public synchronized Path allocateTaskDir(boolean cleanExisting) {

        if (Files.isDirectory(taskDir()) && cleanExisting) {
            log.info(
                "Working directory for name=" + pipelineTask.getId() + " already exists, deleting");
            ZiggyFileUtils.deleteDirectoryTree(taskDir());
        }

        log.info("Creating task working dir: " + taskDir().toString());
        try {
            Files.createDirectories(taskDir());
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create dir " + taskDir().toString(), e);
        }

        return taskDir();
    }
}
