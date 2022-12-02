package gov.nasa.ziggy.buildutil;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.os.OperatingSystem;

/**
 * Compiles the matlab executables (i.e. it runs mcc). This class can not be made final.
 * <p>
 * This class puts the variable MCC_DIR into the environment and sets it to the directory of the project that has
 * invoked this task.
 *
 * @author Bill Wohler
 * @author Sean McCauliff
 * @author Forrest Girouard
 */
public class Mcc extends TessExecTask {

    private static final Logger log = LoggerFactory.getLogger(Mcc.class);

    private FileCollection controllerFiles;
    private FileCollection additionalFiles; // added with mcc -a option
    private File outputExecutable;
    private boolean singleThreaded = true;

    public Mcc() {
        setEnabled(isMccEnabled());
    }

    @InputFiles
    public FileCollection getControllerFiles() {
        return controllerFiles;
    }

    public void setControllerFiles(FileCollection controllerFiles) {
        this.controllerFiles = controllerFiles;
    }

    public FileCollection getAdditionalFiles() {
        return additionalFiles;
    }

    public void setAdditionalFiles(FileCollection additionalFiles) {
        this.additionalFiles = additionalFiles;
    }

    @OutputFile
    public File getOutputExecutable() {
        String path = outputExecutable.getPath();
        if (OperatingSystem.MAC_OS == OperatingSystem.current()) {
            path += ".app";
            return new File(path, "Contents/MacOS/" + outputExecutable.getName());
        }

        return outputExecutable;
    }

    @OutputDirectory
    public File getOutputApplication() {
        String path = outputExecutable.getPath();
        if (OperatingSystem.MAC_OS == OperatingSystem.current()) {
            path += ".app";
            return new File(path);
        }

        return outputExecutable.getParentFile();
    }

    public void setOutputExecutable(File outputExecutable) {
        this.outputExecutable = outputExecutable;
    }

    @Optional
    public boolean isSingleThreaded() {
        return singleThreaded;
    }

    public void setSingleThreaded(boolean newValue) {
        singleThreaded = newValue;
    }

    @TaskAction
    public void action() {
        log.info(String.format("%s.action()\n", this.getClass().getSimpleName()));
        File matlabHome = matlabHome();

        File buildBinDir = new File(getProject().getBuildDir(), "bin");
        List<String> command = new ArrayList<>();

        command.addAll(Arrays.asList("mcc", "-v", "-m", "-N", "-d", buildBinDir.toString(), "-R",
            "-nodisplay", "-R", "-nodesktop"));

        if (isSingleThreaded()) {
            command.add("-R");
            command.add("-singleCompThread");
        }

        for (String s : new String[] { "stats", "signal" }) {
            command.add("-p");
            command.add(matlabHome + "/toolbox/" + s);
        }

        command.add("-o");
        command.add(outputExecutable.getName());

        String path = outputExecutable.getPath();
        File executable = new File(path);
        if (OperatingSystem.MAC_OS == OperatingSystem.current()) {
            path += ".app";
            executable = new File(path);
            String message = String.format(
                "The outputExecutable, \"%s\", already exists and cannot be deleted\n", executable);
            if (executable.exists()) {
                log.info(String.format("%s: already exists, delete it\n", executable));
                if (executable.isDirectory()) {
                    try {
                        FileUtils.deleteDirectory(executable);
                    } catch (IOException e) {
                        log.error(message);
                        throw new GradleException(message, e);
                    }
                } else if (!executable.delete()) {
                    log.error(message);
                    throw new GradleException(message);
                }
            }
            if (executable.exists()) {
                log.error(message);
                throw new GradleException(message);
            }
        }

        if (controllerFiles != null) {
            for (File f : controllerFiles) {
                command.add(f.toString());
            }
        }

        if (additionalFiles != null) {
            for (File f : additionalFiles) {
                command.add("-a");
                command.add(f.toString());
            }
        }

        String cmd = command.stream().collect(Collectors.joining(" "));
        List<String> fullCommand = new ArrayList<>();
        fullCommand.add("/bin/bash");
        fullCommand.add("-c");
        fullCommand.add(cmd);
        cmd = fullCommand.stream().collect(Collectors.joining(" "));
        log.info(cmd);

        ProcessBuilder processBuilder = new ProcessBuilder(fullCommand);
        try {
            processBuilder.environment()
                .put("MCC_DIR", getProject().getProjectDir().getCanonicalPath());
        } catch (IOException e) {
            log.error(String.format("Could not set MCC_DIR: %s", e.getMessage()), e);
        }
        execProcess(processBuilder);

        Set<PosixFilePermission> neededPermissions = new HashSet<>(Arrays.asList(
            new PosixFilePermission[] { OWNER_EXECUTE, GROUP_EXECUTE, OWNER_READ, GROUP_READ }));

        if (!executable.exists()) {
            String message = "The outputExecutable,\"" + executable + "\" does not exist.";
            log.error(message);
            throw new GradleException(message);
        } else {
            Set<PosixFilePermission> currentPosixFilePermissions = null;
            try {
                currentPosixFilePermissions = Files.getPosixFilePermissions(executable.toPath());
                log.info(currentPosixFilePermissions.stream()
                    .map(p -> p.name())
                    .collect(Collectors.joining(" ", "Current file permissions are: ", ".")));
                if (!neededPermissions.containsAll(currentPosixFilePermissions)) {
                    currentPosixFilePermissions.addAll(neededPermissions);
                    log.info(currentPosixFilePermissions.stream()
                        .map(p -> p.name())
                        .collect(Collectors.joining(" ", "Setting file permissions to: ", ",")));
                    Files.setPosixFilePermissions(executable.toPath(), currentPosixFilePermissions);
                }
            } catch (IOException ioe) {
                String message = "Failed to either get or set permissions on outputExecutable \""
                    + outputExecutable;
                log.error(message);
                throw new GradleException(message);
            }
        }
        File readme = new File(outputExecutable.getParentFile(), "readme.txt");
        if (readme.exists()) {
            readme.renameTo(new File(outputExecutable.getParentFile(),
                outputExecutable.getName() + "-readme.txt"));
        }
    }
}
