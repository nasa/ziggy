package gov.nasa.ziggy.data.management;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import gov.nasa.ziggy.module.DatastoreDirectoryPipelineInputs;
import gov.nasa.ziggy.module.DatastoreDirectoryPipelineOutputs;
import gov.nasa.ziggy.module.TaskConfiguration;

/**
 * Test utilities for the data management package. In the main this is class definitions that the
 * tests require.
 *
 * @author PT
 */
public class DataFileTestUtils {

    public static class PipelineInputsSample extends DatastoreDirectoryPipelineInputs {

        private double dvalue;

        /**
         * Since the populateSubtaskInputs() method can do anything, we'll just have it set the
         * dvalue
         */
        public void populateSubtaskInputs() {
            dvalue = 105.3;
        }

        public double getDvalue() {
            return dvalue;
        }

        @Override
        public void copyDatastoreFilesToTaskDirectory(TaskConfiguration taskConfigurationManager,
            Path taskDirectory) {
        }
    }

    public static class PipelineOutputsSample1 extends DatastoreDirectoryPipelineOutputs {

        @Override
        public Set<Path> copyTaskFilesToDatastore() {
            return new HashSet<>();
        }

        @Override
        public boolean subtaskProducedOutputs() {
            return true;
        }
    }
}
