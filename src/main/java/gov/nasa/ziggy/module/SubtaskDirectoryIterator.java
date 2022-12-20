package gov.nasa.ziggy.module;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.SubtaskDirectoryIterator.GroupSubtaskDirectory;

/**
 * Iterates across subtask directories. Returns Pair&#60;groupDir,subtaskDir&#62; for each subtask
 * dir
 *
 * @author Todd Klaus
 * @author PT
 */
public class SubtaskDirectoryIterator implements Iterator<GroupSubtaskDirectory> {
    private static final Logger log = LoggerFactory.getLogger(SubtaskDirectoryIterator.class);
    private static final Pattern SUB_TASK_PATTERN = Pattern.compile("st-([0-9]+)");

    private final Iterator<GroupSubtaskDirectory> dirIterator;
    private final LinkedList<GroupSubtaskDirectory> directoryList;
    private int currentIndex = -1; // the index of the last item retured by next()

    public SubtaskDirectoryIterator(File taskDirectory) {
        directoryList = new LinkedList<>();
        buildDirectoryList(taskDirectory);
        log.debug("Number of subtask directories detected in task directory "
            + taskDirectory.toString() + ": " + directoryList.size());
        dirIterator = directoryList.iterator();
    }

    private void buildDirectoryList(File dir) {
        List<File> files = listSubtaskDirsNumericallyOrdered(dir);

        for (File file : files) {
            File groupDir = file.getParentFile();
            File subtaskDir = file;
            directoryList.add(new GroupSubtaskDirectory(groupDir, subtaskDir));
            log.debug("Adding: " + file);
        }
    }

    /**
     * Return a list of Files in numeric order. Assumes all file names are of the form st-n, where n
     * is an integer. File names that do not match this format are ignored and are not returned in
     * the list.
     *
     * @param dir
     * @return
     */
    private List<File> listSubtaskDirsNumericallyOrdered(File dir) {
        File[] files = dir.listFiles();
        Map<Integer, File> orderedMap = new TreeMap<>();

        for (File file : files) {
            int n = subtaskNumber(file.getName());
            if (n >= 0 && file.isDirectory()) {
                orderedMap.put(n, file);
            }
        }

        List<File> orderedList = new LinkedList<>();

        for (int n : orderedMap.keySet()) {
            orderedList.add(orderedMap.get(n));
        }

        return orderedList;
    }

    private int subtaskNumber(String name) {
        Matcher matcher = SUB_TASK_PATTERN.matcher(name);
        int number = -1;
        if (matcher.matches()) {
            number = Integer.parseInt(matcher.group(1));
        }
        return number;
    }

    public int getCurrentIndex() {
        return currentIndex;
    }

    public int numSubTasks() {
        return directoryList.size();
    }

    @Override
    public boolean hasNext() {
        return dirIterator.hasNext();
    }

    @Override
    public GroupSubtaskDirectory next() {
        currentIndex++;
        return dirIterator.next();
    }

    @Override
    public void remove() {
        throw new IllegalStateException("remove() not supported");
    }

    public static class GroupSubtaskDirectory {

        private final File groupDir;
        private final File subtaskDir;

        public GroupSubtaskDirectory(File groupDir, File subtaskDir) {
            this.groupDir = groupDir;
            this.subtaskDir = subtaskDir;
        }

        public File getGroupDir() {
            return groupDir;
        }

        public File getSubtaskDir() {
            return subtaskDir;
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupDir, subtaskDir);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            GroupSubtaskDirectory other = (GroupSubtaskDirectory) obj;
            return Objects.equals(groupDir, other.groupDir)
                && Objects.equals(subtaskDir, other.subtaskDir);
        }
    }
}
