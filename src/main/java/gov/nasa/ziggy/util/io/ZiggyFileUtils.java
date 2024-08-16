package gov.nasa.ziggy.util.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;

/**
 * Some handy methods for dealing with files or groups of files.
 *
 * @author Sean McCauliff
 * @author PT
 */
public class ZiggyFileUtils {

    static final int BUFFER_SIZE = 1000;

    private static final Logger log = LoggerFactory.getLogger(ZiggyFileUtils.class);

    public static final String FILE_OVERWRITE_PERMISSIONS = "rw-rw-r--";
    public static final String DIR_OVERWRITE_PERMISSIONS = "rwxrwxr-x";
    public static final String FILE_READONLY_PERMISSIONS = "r--r--r--";
    public static final String DIR_READONLY_PERMISSIONS = "r-xr-xr-x";

    public static final Charset ZIGGY_CHARSET = StandardCharsets.UTF_8;

    public static final String ZIGGY_CHARSET_NAME = ZIGGY_CHARSET.name();

    /**
     * Applies write protection to a directory tree. All directories will have permissions set to
     * {@link #DIR_READONLY_PERMISSIONS}; all regular files will have permissions set to
     * {@link #FILE_READONLY_PERMISSIONS}.
     *
     * @param top root of directory tree.
     */
    public static void writeProtectDirectoryTree(Path top) {
        setPosixPermissionsRecursively(top, FILE_READONLY_PERMISSIONS, DIR_READONLY_PERMISSIONS);
    }

    /**
     * Prepares a directory tree for overwrites. All directories will have permissions set to
     * {@link #DIR_OVERWRITE_PERMISSIONS}; all regular files will have permissions set to
     * {@link #FILE_OVERWRITE_PERMISSIONS}.
     *
     * @param top root of directory tree.
     */
    public static void prepareDirectoryTreeForOverwrites(Path top) {
        setPosixPermissionsRecursively(top, FILE_OVERWRITE_PERMISSIONS, DIR_OVERWRITE_PERMISSIONS);
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
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
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
                    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
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
                    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
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

    /**
     * Locates all the regular files under a root directory. This includes both real regular files
     * and symbolic links to regular files. The result is returned as a {@link Map} in which the
     * {@link Path} to the file under the root directory is the key and the {@link Path} to the real
     * regular file is the value. The key will be relative to the root directory, while the value
     * will be an absolute path.
     * <p>
     * A note on symbolic links: when a real directory that contains a symbolic link is found, this
     * code will do what is expected (i.e., it will put the real file target of the link into the
     * map as the value that goes with the symbolic link file key). When a symbolic link directory
     * that contains a symbolic link file is found, likewise. However: real files that are in a
     * directory that is the target of a symbolic link will do something slightly unexpected. In
     * that case, the real directory is not resolved when generating the value for the map. In other
     * words: if directory /bar is a link to directory /foo, and directory /foo contains real file
     * baz, the map will contain /bar/baz as the key AND /bar/baz as the value, rather than /foo/baz
     * as the value.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static Map<Path, Path> regularFilesInDirTree(Path rootDir) {

        Map<Path, Path> regularFiles = new HashMap<>();
        try {
            // Walk the file tree.
            Files.walkFileTree(rootDir, ImmutableSet.of(FileVisitOption.FOLLOW_LINKS),
                Integer.MAX_VALUE, new FileVisitor<Path>() {

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {

                        // Add the file and its real source to the map.
                        try {
                            Path realFile = ZiggyFileUtils.realSourceFile(file);
                            if (Files.isDirectory(realFile) || Files.isHidden(file)) {
                                return FileVisitResult.CONTINUE;
                            }
                            regularFiles.put(rootDir.relativize(file), realFile.toAbsolutePath());
                            return FileVisitResult.CONTINUE;
                        } catch (IOException e) {
                            throw new UncheckedIOException(
                                "Unable to test whether file " + file.toString() + " is hidden", e);
                        }
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) {
                        log.error("Unable to visit file " + file + " for checksum purposes.", exc);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                        return FileVisitResult.CONTINUE;
                    }
                });
            return regularFiles;
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to walk file tree  " + rootDir.toString(), e);
        }
    }

    /**
     * Assumes ASCII in the input stream.
     *
     * @param bldr
     * @param in
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void readAll(Appendable bldr, InputStream in) {
        try {
            for (int byteValue = in.read(); byteValue != -1; byteValue = in.read()) {
                char c = (char) byteValue;
                bldr.append(c);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read input stream", e);
        }
    }

    /**
     * Makes symlink targets in a destination directory of files in a source directory. The
     * operation is not recursive (i.e., if there's a subdirectory in the source directory, it will
     * get symlinked to the destination).
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void symlinkDirectoryContents(Path src, Path dest) {
        try {
            if (!Files.exists(src) || !Files.isDirectory(src)) {
                throw new IllegalArgumentException(
                    "Source \"" + src + "\"does not exist or is not a directory.");
            }
            if (Files.exists(dest) && !Files.isDirectory(dest)) {
                throw new IllegalArgumentException(
                    "Destination \"" + dest + "\"exists but is not a directory.");
            }
            if (!Files.exists(dest)) {
                Files.createDirectories(dest);
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(src)) {
                for (Path entry : stream) {
                    String srcName = entry.getFileName().toString();
                    Files.createSymbolicLink(dest.resolve(srcName), entry);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to symlink " + src.toString() + " to " + dest.toString(), e);
        }
    }

    /**
     * Calls {@code close} method on given non-null instances of {@code Closeable} ignoring
     * {@code IOException}s.
     *
     * @param closeable object upon which to invoke {@code close} method. This may be null in which
     * case it is ignored.
     */
    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }
    }

    public static String modeToPosixFileString(int mode) {

        StringBuilder posixSb = new StringBuilder();
        // Convert the number to a String.
        String modeCharArray = Integer.toString(mode);

        // left-pad with zeros to get to 3 digits
        while (modeCharArray.length() < 3) {
            modeCharArray = "0" + modeCharArray;
        }

        // Loop over the digits and determine the individual permissions
        for (int i = 0; i < modeCharArray.length(); i++) {
            int modeEntry = Integer.parseInt(modeCharArray.substring(i, i + 1));
            posixSb.append(modeEntry % 2 == 0 ? "-" : "r");
            modeEntry /= 2;
            posixSb.append(modeEntry % 2 == 0 ? "-" : "w");
            modeEntry /= 2;
            posixSb.append(modeEntry % 2 == 0 ? "-" : "x");
        }
        return posixSb.toString();
    }

    public static Set<PosixFilePermission> modeToPosixFilePermissions(int mode) {
        return PosixFilePermissions.fromString(modeToPosixFileString(mode));
    }

    /**
     * Recursively cleans a directory tree. Each subdirectory is emptied and then deleted; the
     * top-level directory is emptied but not deleted. Useful because the Apache FileUtils methods
     * that purport to do this don't work correctly on all platforms.
     * <p>
     * Note that the standard {@link Files#walkFileTree(Path, FileVisitor)} method is not acceptable
     * in this case because it doesn't perform visiting in a logical manner. In particular, it tries
     * to do its post-visit on the top-level directory before going to the lower-level ones, which
     * puts us back at trying to delete non-empty directories.
     *
     * @param directory top-level directory.
     * @param force indicates that permissions should be changed, if necessary, to delete files and
     * directories.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void cleanDirectoryTree(Path directory, boolean force) {
        if (force) {
            prepareDirectoryTreeForOverwrites(directory);
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                if (Files.isDirectory(file) && !Files.isSymbolicLink(file)) {
                    cleanDirectoryTree(file);
                }
                Files.delete(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to clean directory tree  " + directory.toString(), e);
        }
    }

    public static void cleanDirectoryTree(Path directory) {
        cleanDirectoryTree(directory, false);
    }

    public static void deleteDirectoryTree(Path directory) {
        deleteDirectoryTree(directory, false);
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static void deleteDirectoryTree(Path directory, boolean force) {
        if (!Files.exists(directory)) {
            return;
        }
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException(
                "File " + directory.toString() + " is not a directory");
        }
        cleanDirectoryTree(directory, force);
        try {
            if (force) {
                Files.setPosixFilePermissions(directory,
                    PosixFilePermissions.fromString(DIR_OVERWRITE_PERMISSIONS));
            }
            FileUtils.deleteDirectory(directory.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to delete directory " + directory.toString(), e);
        }
    }

    /**
     * Finds the actual source file for a given source file. If the source file is not a symbolic
     * link, then that file is the actual source file. If not, the symbolic link is read to find the
     * actual source file. The reading of symbolic links runs iteratively, so it produces the
     * correct result even in the case of a link to a link to a link... etc. The process of
     * following symbolic links stops at the first such link that is a child of the datastore root
     * path. Thus the "actual source" is either a non-symlink file that the src file is a link to,
     * or it's a file (symlink or regular file) that lies inside the datastore.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static Path realSourceFile(Path src) {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();
        Path trueSrc = src;
        if (Files.isSymbolicLink(src) && !src.startsWith(datastoreRoot)) {
            try {
                trueSrc = realSourceFile(Files.readSymbolicLink(src));
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to resolve symbolic link " + src.toString(),
                    e);
            }
        }
        return trueSrc;
    }

    /** Abstraction of the {@link Files#list(Path)} API for a fast, simple directory listing. */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static Set<Path> listFiles(Path directory) {
        return listFiles(directory, null, null);
    }

    /**
     * Abstraction of the {@link Files#list(Path)} API for a fast, simple directory listing that
     * filters results according to a regular expression.
     */
    public static Set<Path> listFiles(Path directory, String regexp) {
        return listFiles(directory, Set.of(Pattern.compile(regexp)), null);
    }

    /**
     * Abstraction of the {@link Files#list(Path)} API for a fast, simple directory listing that
     * filters results according to two collections of {@link Pattern}s. The first collection is of
     * Patterns that must be matched (i.e., include patterns); the second collection is of Patterns
     * that must not be matched (i.e., exclude patterns). Any file that matches both an include and
     * an exclude pattern will be excluded. Either collection of Patterns can be empty, or null.
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public static Set<Path> listFiles(Path directory, Collection<Pattern> includePatterns,
        Collection<Pattern> excludePatterns) {
        try (Stream<Path> stream = Files.list(directory)) {
            Stream<Path> filteredStream = stream;
            if (!CollectionUtils.isEmpty(includePatterns)) {
                for (Pattern includePattern : includePatterns) {
                    filteredStream = filteredStream
                        .filter(s -> includePattern.matcher(s.getFileName().toString()).matches());
                }
            }
            if (!CollectionUtils.isEmpty(excludePatterns)) {
                for (Pattern excludePattern : excludePatterns) {
                    filteredStream = filteredStream
                        .filter(s -> !excludePattern.matcher(s.getFileName().toString()).matches());
                }
            }
            return filteredStream.collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Enum-with-behavior that supports multiple different copy mechanisms that are specialized for
     * use with moving files between the datastore and a working directory. The following options
     * are provided:
     * <ol>
     * <li>{@link CopyType#COPY} performs a traditional file copy operation. The copy is recursive,
     * so directories are supported as well as individual files.
     * <li>{@link CopyType#LINK} makes the destination a hard link to the true source file, as
     * defined by the {@link realSourceFile} method. Linking can be faster than copying and can
     * consume less disk space (assuming the datastore and working directories are on the same file
     * system).
     * <li>{@link CopyType#MOVE} will move the true source file to the destination; that is, it will
     * follow symlinks via the {@link realSourceFile} method and move the file that is found in this
     * way. In addition, if the source file is a symlink, it will be changed to a symlink to the
     * moved file in its new location. In this way, the source file symlink remains valid and
     * unchanged, but it now targets the moved file. to the moved file.
     * </ol>
     * In addition to all the foregoing, {@link CopyType} manages file permissions. After execution
     * of any move / copy / symlink operation, the new file's permissions are set to make it
     * write-protected and world-readable. If the copy / move / symlink operation is required to
     * overwrite the destination file, that file's permissions will be set to allow the overwrite
     * prior to execution.
     * <p>
     * For copying files from the datastore to a subtask directory, {@link CopyType#COPY}, and
     * {@link CopyType#LINK} options are available. For copies from the subtask directory to the
     * datastore, {@link CopyType#MOVE} and {@link CopyType#LINK} are available.
     *
     * @author PT
     */
    public enum CopyType {
        COPY {
            @Override
            @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
            protected void copyInternal(Path src, Path dest) {
                try {
                    checkArguments(src, dest);
                    if (Files.isRegularFile(src)) {
                        Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
                    } else {
                        FileUtils.copyDirectory(src.toFile(), dest.toFile());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to copy " + src.toString() + " to " + dest.toString(), e);
                }
            }
        },
        MOVE {
            @Override
            @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
            protected void copyInternal(Path src, Path dest) {
                try {
                    checkArguments(src, dest);
                    Path trueSrc = realSourceFile(src);
                    Files.move(trueSrc, dest, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                    if (src != trueSrc) {
                        Files.createSymbolicLink(src, dest);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to move " + src.toString() + " to " + dest.toString(), e);
                }
            }
        },
        LINK {
            @Override
            @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
            protected void copyInternal(Path src, Path dest) {
                try {
                    checkArguments(src, dest);
                    Path trueSrc = realSourceFile(src);
                    if (Files.exists(dest)) {
                        Files.delete(dest);
                    }
                    createLink(trueSrc, dest);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to link from " + src.toString() + " to " + dest.toString(), e);
                }
            }

            /** Recursively copies directories and hard-links regular files. */
            private void createLink(Path src, Path dest) throws IOException {
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dest);
                    for (Path file : Files.list(src).collect(Collectors.toList())) {
                        createLink(file, dest.resolve(file.getFileName()));
                    }
                } else {
                    Files.createLink(dest, src);
                }
            }
        };

        /**
         * Copy operation that allows / forces the caller to manage any {@link IOException} that
         * occurs.
         */
        protected abstract void copyInternal(Path src, Path dest);

        /**
         * Copy operation that manages any resulting {@link IOException}. In this event, an
         * {@link UncheckedIOException} is thrown, which terminates execution of the datastore
         * operations.
         */
        public void copy(Path src, Path dest) {
            copyInternal(src, dest);
        }

        private static void checkArguments(Path src, Path dest) {
            checkNotNull(src, "src");
            checkNotNull(dest, "dest");
            checkArgument(Files.exists(src, LinkOption.NOFOLLOW_LINKS),
                "Source file " + src + " does not exist");
        }
    }
}
