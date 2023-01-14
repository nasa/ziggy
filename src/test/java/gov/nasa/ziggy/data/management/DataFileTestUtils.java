package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import gov.nasa.ziggy.module.PipelineInputs;
import gov.nasa.ziggy.module.PipelineOutputs;
import gov.nasa.ziggy.module.PipelineResults;
import gov.nasa.ziggy.module.TaskConfigurationManager;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Test utilities for the data management package. In the main this is class definitions that the
 * tests require.
 *
 * @author PT
 */
public class DataFileTestUtils {

    /**
     * Subclass of DataFileInfo class used to exercise the class features unit tests. It accepts
     * files with the name pattern: "pa-<9-digit-number>-<number>-results.h5".
     *
     * @author PT
     */
    public static class DataFileInfoSample1 extends DataFileInfo {

        private static final Pattern PATTERN = Pattern.compile("pa-\\d{9}-\\d+-results.h5");

        public DataFileInfoSample1() {
        }

        public DataFileInfoSample1(Path file) {
            super(file);
        }

        public DataFileInfoSample1(String name) {
            super(name);
        }

        /**
         * Provides a Pattern that expects a String with a form sort of like:
         * "pa-001234567-10-results.h5", where the first set of numbers is exactly 9 digits, but the
         * second set can be any length.
         */
        @Override
        protected Pattern getPattern() {
            return PATTERN;
        }

    }

    /**
     * Subclass of DataFileInfo class used to exercise features in unit tests. It accepts files with
     * the name pattern "cal-#-#-L-<number>-results.h5", where "L" is a capital letter from the set
     * "ABCD".
     *
     * @author PT
     */
    static class DataFileInfoSample2 extends DataFileInfo {

        private static final Pattern PATTERN = Pattern
            .compile("cal-\\d{1}-\\d{1}-[ABCD]-\\d+-results.h5");

        public DataFileInfoSample2() {
        }

        public DataFileInfoSample2(Path file) {
            super(file);
        }

        public DataFileInfoSample2(String name) {
            super(name);
        }

        /**
         * Provides a Pattern that expects a string with a form sort of like:
         * "cal-1-1-A-20-results.h5". The first 2 numbers are 1 digit exactly, the letter is one of
         * "ABCD", the final number can be any length.
         */
        @Override
        protected Pattern getPattern() {
            return PATTERN;
        }

    }

    /**
     * DataFileInfo class for testing full-directory copying.
     *
     * @author PT
     */
    public static class DataFileInfoSampleForDirs extends DataFileInfo {

        public DataFileInfoSampleForDirs() {
        }

        public DataFileInfoSampleForDirs(String name) {
            super(name);
        }

        public DataFileInfoSampleForDirs(Path name) {
            super(name);
        }

        // Use the horrendous directory pattern used by Hyperion L0 data directories
        private static Pattern PATTERN = Pattern
            .compile("EO1H([0-9]{6})([0-9]{4})([0-9]{3})([A-Z0-9]{5})_([A-Z]{3})_([0-9]{2})");

        @Override
        protected Pattern getPattern() {
            return PATTERN;
        }

    }

    /**
     * PipelineResults subclass for test purposes.
     *
     * @author PT
     */
    public static class PipelineResultsSample1 extends PipelineResults {

        private int value;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

    }

    /**
     * PipelineResults subclass for test purposes.
     *
     * @author PT
     */
    public static class PipelineResultsSample2 extends PipelineResults {

        private float fvalue;

        public float getFvalue() {
            return fvalue;
        }

        public void setFvalue(float fvalue) {
            this.fvalue = fvalue;
        }
    }

    public static class PipelineInputsSample extends PipelineInputs {

        private double dvalue;

        /**
         * For test purposes, we will make the DatastoreIdSample1 class the only one that is
         * required to populate PipelineInputsSample
         */
        @Override
        public Set<Class<? extends DataFileInfo>> requiredDataFileInfoClasses() {
            Set<Class<? extends DataFileInfo>> requiredClasses = new HashSet<>();
            requiredClasses.add(DataFileInfoSample1.class);
            return requiredClasses;
        }

        /**
         * Since the populateSubTaskInputs() method can do anything, we'll just have it set the
         * dvalue
         */
        @Override
        public void populateSubTaskInputs() {
            dvalue = 105.3;
        }

        public double getDvalue() {
            return dvalue;
        }

        @Override
        public DatastorePathLocator datastorePathLocator(PipelineTask pipelineTask) {
            return null;
        }

        @Override
        public void copyDatastoreFilesToTaskDirectory(
            TaskConfigurationManager taskConfigurationManager, PipelineTask pipelineTask,
            Path taskDirectory) {
        }

        @Override
        public Set<Path> findDatastoreFilesForInputs(PipelineTask pipelineTask) {
            return Collections.emptySet();
        }

    }

    public static class PipelineOutputsSample1 extends PipelineOutputs {

        private int[] ivalues;

        @Override
        public void populateTaskResults() {
            /**
             * Since the populateTaskResults() value can do anything, we'll use it to set the
             * ivalues
             */
            ivalues = new int[] { 27, -9, 5 };
        }

        public int[] getIvalues() {
            return ivalues;
        }

        public void setIvalues(int[] ivalues) {
            this.ivalues = ivalues;
        }

        @Override
        public Map<DataFileInfo, PipelineResults> pipelineResults() {
            Map<DataFileInfo, PipelineResults> map = new HashMap<>();

            // all the results files will use the DataFileInfoSample1 class and
            // will be of the PipelineResultsSample1 class

            int i = 0;
            for (int f : getIvalues()) {
                String fname = String.format("pa-001234567-%d-results.h5", i);
                DataFileInfoSample1 d = new DataFileInfoSample1(fname);
                PipelineResultsSample1 p = new PipelineResultsSample1();
                p.setValue(f);
                map.put(d, p);
                i++;
            }

            return map;
        }

        @Override
        public void updateInputFileConsumers(PipelineInputs pipelineInputs,
            PipelineTask pipelineTask, Path taskDirectory) {
        }

        @Override
        protected boolean subtaskProducedResults() {
            return true;
        }

    }

    /**
     * DatastorePathLocator implementation for test purposes. For instances of DatastoreInfoSample1,
     * it returns the combination of the datastore root and the pa-<number> as the path for the
     * file; for instance of DatastoreInfoSample2, it returns the datastore root and the cal-#-#-L
     * as the path for the file.
     *
     * @author PT
     */
    public static class DatastorePathLocatorSample implements DatastorePathLocator {

        public DatastorePathLocatorSample() {
        }

        @Override
        public Path datastorePath(DataFileInfo dataFileInfo) {
            Path datastoreRoot = DirectoryProperties.datastoreRootDir();
            Path p = null;
            String s = dataFileInfo.getName().toString();
            if (dataFileInfo instanceof DataFileInfoSample1) {
                p = datastoreRoot.resolve("pa").resolve("20").resolve(s);
            } else if (dataFileInfo instanceof DataFileInfoSample2) {
                p = datastoreRoot.resolve("cal").resolve("20").resolve(s);
            } else if (dataFileInfo instanceof DataFileInfoSampleForDirs) {
                p = datastoreRoot.resolve(s);
            }
            return p;
        }

    }

    public static final DataFileType dataFileTypeSample1 = new DataFileType();
    public static final DataFileType dataFileTypeSample2 = new DataFileType();

    public static void initializeDataFileTypeSamples() {
        dataFileTypeSample1.setName("pa");
        dataFileTypeSample1.setFileNameRegexForTaskDir("pa-([0-9]{9})-([0-9]{2})-results.h5");
        dataFileTypeSample1.setFileNameWithSubstitutionsForDatastore("pa/$2/pa-$1-$2-results.h5");
        dataFileTypeSample2.setName("cal");
        dataFileTypeSample2
            .setFileNameRegexForTaskDir("cal-([1-4])-([1-4])-([ABCD])-([0-9]{2})-results.h5");
        dataFileTypeSample2
            .setFileNameWithSubstitutionsForDatastore("cal/$4/cal-$1-$2-$3-$4-results.h5");
    }

    public static final DataFileType dataFileTypeForDirectories = new DataFileType();

    public static void initializeDataFileTypeForDirectories() {
        dataFileTypeForDirectories.setName("Hyperion L0");
        dataFileTypeForDirectories.setFileNameRegexForTaskDir(
            "EO1H([0-9]{6})([0-9]{4})([0-9]{2})([0-9]{1})([A-Z0-9]{5})_([A-Z]{3})_([0-9]{2})");
        dataFileTypeForDirectories.setFileNameWithSubstitutionsForDatastore("EO1H$1$2$3$4$5_$6_$7");
    }

}
