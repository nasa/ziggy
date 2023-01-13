package gov.nasa.ziggy.util.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import gov.nasa.ziggy.data.management.DataFileManager;

/**
 * Some handy methods for dealing with files or groups of files.
 *
 * @author Sean McCauliff
 * @author PT
 */
public class FileUtil {
    static final int BUFFER_SIZE = 1000;

    private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

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
     * @throws IOException
     */
    public static void writeProtectDirectoryTree(Path top) throws IOException {
        setPosixPermissionsRecursively(top, FILE_READONLY_PERMISSIONS, DIR_READONLY_PERMISSIONS);
    }

    /**
     * Prepares a directory tree for overwrites. All directories will have permissions set to
     * {@link #DIR_OVERWRITE_PERMISSIONS}; all regular files will have permissions set to
     * {@link #FILE_OVERWRITE_PERMISSIONS}.
     *
     * @param top root of directory tree.
     * @throws IOException
     */
    public static void prepareDirectoryTreeForOverwrites(Path top) throws IOException {
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
    public static void setPosixPermissionsRecursively(Path top, String filePermissions,
        String dirPermissions) throws IOException {
        if (!Files.isDirectory(top)) {
            Files.setPosixFilePermissions(top, PosixFilePermissions.fromString(filePermissions));
        } else {
            Files.setPosixFilePermissions(top, PosixFilePermissions.fromString(dirPermissions));
            Files.walkFileTree(top, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                    Files.setPosixFilePermissions(dir,
                        PosixFilePermissions.fromString(dirPermissions));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                    if (Files.isSymbolicLink(file)) {
                        return FileVisitResult.CONTINUE;
                    }
                    Files.setPosixFilePermissions(file,
                        PosixFilePermissions.fromString(filePermissions));
                    return FileVisitResult.CONTINUE;
                }
            });
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
     *
     * @throws IOException
     */
    public static Map<Path, Path> regularFilesInDirTree(Path rootDir) throws IOException {

        Map<Path, Path> regularFiles = new HashMap<>();

        // Walk the file tree.
        Files.walkFileTree(rootDir, ImmutableSet.of(FileVisitOption.FOLLOW_LINKS),
            Integer.MAX_VALUE, new FileVisitor<Path>() {

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {

                    // Add the file and its real source to the map.
                    Path realFile = DataFileManager.realSourceFile(file);
                    if (Files.isDirectory(realFile) || Files.isHidden(file)) {
                        return FileVisitResult.CONTINUE;
                    }
                    regularFiles.put(rootDir.relativize(file), realFile.toAbsolutePath());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException {
                    log.error("Unable to visit file " + file + " for checksum purposes.", exc);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

        return regularFiles;
    }

    /**
     * Assumes ASCII in the input stream.
     *
     * @param bldr
     * @param in
     * @throws IOException
     */
    public static void readAll(Appendable bldr, InputStream in) throws IOException {
        for (int byteValue = in.read(); byteValue != -1; byteValue = in.read()) {
            char c = (char) byteValue;
            bldr.append(c);
        }
    }

    /**
     * Makes symlink targets in a destination directory of files in a source directory. The
     * operation is not recursive (i.e., if there's a subdirectory in the source directory, it will
     * get symlinked to the destination).
     */
    public static void symlinkDirectoryContents(Path src, Path dest) throws IOException {
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
     * @throws IOException
     */
    public static void cleanDirectoryTree(Path directory, boolean force) throws IOException {
        if (force) {
            setPosixPermissionsRecursively(directory, "rw-r--r--", "rwxr-xr-x");
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                if (Files.isDirectory(file)) {
                    cleanDirectoryTree(file, force);
                }
                Files.delete(file);
            }
        }
    }

    public static void cleanDirectoryTree(Path directory) throws IOException {
        cleanDirectoryTree(directory, false);
    }

    public static void deleteDirectoryTree(Path directory) throws IOException {
        deleteDirectoryTree(directory, false);
    }

    public static void deleteDirectoryTree(Path directory, boolean force) throws IOException {
        cleanDirectoryTree(directory, force);
        if (force) {
            setPosixPermissionsRecursively(directory, "rw-r--r--", "rwxr-xr-x");
        }
        FileUtils.deleteDirectory(directory.toFile());
    }

}
