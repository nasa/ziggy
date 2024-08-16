package gov.nasa.ziggy.buildutil;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.os.OperatingSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compiles the matlab executables (i.e. it runs mcc). This class can not be made final.
 * <p>
 * This class puts the variable MCC_DIR into the environment and sets it to the directory of the
 * project that has invoked this task.
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
    @Optional
    public FileCollection getControllerFiles() {
        return controllerFiles;
    }

    public void setControllerFiles(FileCollection controllerFiles) {
        this.controllerFiles = controllerFiles;
    }

    @InputFiles
    @Optional
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
    @Input
    public Boolean isSingleThreaded() {
        return singleThreaded;
    }

    public void setSingleThreaded(boolean newValue) {
        singleThreaded = newValue;
    }

    public Set<File> controllerFiles() {
        return controllerFiles != null ? controllerFiles.getFiles() : new HashSet<>();
    }

    public Set<File> additionalFiles() {
        return additionalFiles != null ? additionalFiles.getFiles() : new HashSet<>();
    }

    @TaskAction
    public void action() {
        log.info(String.format("%s.action()\n", this.getClass().getSimpleName()));
        File matlabHome = matlabHome();

        File buildBinDir = new File(getProject().getBuildDir(), "bin");
        List<String> command = new ArrayList<>(Arrays.asList("mcc", "-v", "-m", "-N", "-d",
            buildBinDir.toString(), "-R", "-nodisplay", "-R", "-nodesktop"));

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

        if (controllerFiles().isEmpty()) {
            throw new GradleException("No controller files identified");
        }
        for (File f : controllerFiles()) {
            command.add(f.toString());
        }

        if (!additionalFiles().isEmpty()) {
            for (File f : additionalFiles()) {
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

        Set<PosixFilePermission> neededPermissions = new HashSet<>(
            Arrays.asList(OWNER_EXECUTE, GROUP_EXECUTE, OWNER_READ, GROUP_READ));

        if (!executable.exists()) {
            String message = "The outputExecutable,\"" + executable + "\" does not exist.";
            log.error(message);
            throw new GradleException(message);
        }
        Set<PosixFilePermission> currentPosixFilePermissions = null;
        try {
            currentPosixFilePermissions = Files.getPosixFilePermissions(executable.toPath());
            log.info(currentPosixFilePermissions.stream()
                .map(PosixFilePermission::name)
                .collect(Collectors.joining(" ", "Current file permissions are: ", ".")));
            if (!neededPermissions.containsAll(currentPosixFilePermissions)) {
                currentPosixFilePermissions.addAll(neededPermissions);
                log.info(currentPosixFilePermissions.stream()
                    .map(PosixFilePermission::name)
                    .collect(Collectors.joining(" ", "Setting file permissions to: ", ",")));
                Files.setPosixFilePermissions(executable.toPath(), currentPosixFilePermissions);
            }
        } catch (IOException ioe) {
            String message = "Failed to either get or set permissions on outputExecutable \""
                + outputExecutable;
            log.error(message);
            throw new GradleException(message);
        }

        // On the Mac, there are 2 files created without U+W permission, so any time the
        // user tries an incremental build it fails because those files can't be deleted.
        if (SystemArchitecture.architecture() == SystemArchitecture.MAC_INTEL
            || SystemArchitecture.architecture() == SystemArchitecture.MAC_M1) {
            setPosixPermissionsRecursively(executable.toPath().resolve("Contents"), "rwxr-xr--",
                "rwxr-xr--");
        }
        File readme = new File(outputExecutable.getParentFile(), "readme.txt");
        if (readme.exists()) {
            readme.renameTo(new File(outputExecutable.getParentFile(),
                outputExecutable.getName() + "-readme.txt"));
        }
    }

    /**
     * Recursively sets permissions on all files and directories that lie under a given top-level
     * directory.
     *
     * @param top Location of the top-level directory.
     * @param filePermissions POSIX-style string of permissions for regular files (i.e.,
     * "r--r-r--").
     * @param dirPermissions POSIX-style string of permissions for regular files (i.e.,
     * "rwxr-xr-x").
     * <p>
     * NB: This is a copy of the same method that appears in FileUtil. It's duplicated here so that
     * we don't need to have buildSrc trying to use code from Ziggy main source, and also so that we
     * don't wind up with code in FileUtil calling this method in buildSrc.
     */
    public static void setPosixPermissionsRecursively(Path top, String filePermissions,
        String dirPermissions) {
        try {
            if (!Files.isDirectory(top)) {
                Files.setPosixFilePermissions(top,
                    PosixFilePermissions.fromString(filePermissions));
            } else {
                Files.setPosixFilePermissions(top, PosixFilePermissions.fromString(dirPermissions));
                Files.walkFileTree(top, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        try {
                            Files.setPosixFilePermissions(dir,
                                PosixFilePermissions.fromString(dirPermissions));
                        } catch (IOException e) {
                            throw new UncheckedIOException(
                                "Failed to set permissions on dir " + dir.toString(), e);
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (Files.isSymbolicLink(file)) {
                            return FileVisitResult.CONTINUE;
                        }
                        try {
                            Files.setPosixFilePermissions(file,
                                PosixFilePermissions.fromString(filePermissions));
                        } catch (IOException e) {
                            throw new UncheckedIOException(
                                "Failed to set permissions on file " + file.toString(), e);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to set permissions on dir " + top.toString(), e);
        }
    }
}
