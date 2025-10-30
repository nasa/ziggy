package gov.nasa.ziggy.pipeline.step.subtask;

import java.io.File;

import gov.nasa.ziggy.pipeline.step.TaskConfiguration;

/** Factory class for {@link SubtaskExecutor} instances. */
public class SubtaskExecutorFactory {

    public static final String PYTHON_SUFFIX = ".py";

    /**
     * Enum-with-behaviors that returns an appropriate {@link SubtaskExecutor} subclass for one of
     * the algorithm languages that has specialized support in Ziggy.
     *
     * @author PT
     */
    private enum SubtaskExecutorType {
        PYTHON {
            @Override
            // Binary name ends with ".py", hence is a Python module.
            public boolean correctSubtaskExecutorType(SubtaskMaster subtaskMaster) {
                return subtaskMaster.getBinaryName().endsWith(PYTHON_SUFFIX);
            }

            @Override
            public SubtaskExecutor subtaskExecutor(File taskDir, int subtaskIndex,
                String binaryName, int timeoutSecs, float heapSizeGigabytes,
                TaskConfiguration taskConfiguration) {
                return new PythonSubtaskExecutor(taskDir, subtaskIndex, binaryName, timeoutSecs,
                    heapSizeGigabytes, taskConfiguration);
            }
        },
        MATLAB {
            @Override
            public boolean correctSubtaskExecutorType(SubtaskMaster subtaskMaster) {
                // Look for the "requiredMCRProducts.txt" file for the algorithm.
                return SubtaskExecutor.binaryDir(SubtaskExecutor.binPath(),
                    subtaskMaster.getBinaryName() + "-requiredMCRProducts.txt") != null;
            }

            @Override
            public SubtaskExecutor subtaskExecutor(File taskDir, int subtaskIndex,
                String binaryName, int timeoutSecs, float heapSizeGigabytes,
                TaskConfiguration taskConfiguration) {
                return new MatlabSubtaskExecutor(taskDir, subtaskIndex, binaryName, timeoutSecs,
                    heapSizeGigabytes, taskConfiguration);
            }
        },
        JAVA {
            @Override
            public boolean correctSubtaskExecutorType(SubtaskMaster subtaskMaster) {
                // See if the binary name can be interpreted as a class name.
                try {
                    Class.forName(subtaskMaster.getBinaryName());
                    return true;
                } catch (ClassNotFoundException e) {
                    return false;
                }
            }

            @Override
            public SubtaskExecutor subtaskExecutor(File taskDir, int subtaskIndex,
                String binaryName, int timeoutSecs, float heapSizeGigabytes,
                TaskConfiguration taskConfiguration) {
                return new JavaSubtaskExecutor(taskDir, subtaskIndex, binaryName, timeoutSecs,
                    heapSizeGigabytes, taskConfiguration);
            }
        };

        public SubtaskExecutor subtaskExecutor(SubtaskMaster subtaskMaster) {
            return correctSubtaskExecutorType(subtaskMaster)
                ? subtaskExecutor(subtaskMaster.taskDir(), subtaskMaster.getSubtaskIndex(),
                    subtaskMaster.getBinaryName(), subtaskMaster.getTimeoutSecs(),
                    subtaskMaster.getHeapSizeGigabytes(), subtaskMaster.getTaskConfiguration())
                : null;
        }

        public abstract SubtaskExecutor subtaskExecutor(File taskDir, int subtaskIndex,
            String binaryName, int timeoutSecs, float heapSizeGigabytes,
            TaskConfiguration taskConfiguration);

        public abstract boolean correctSubtaskExecutorType(SubtaskMaster subtaskMaster);
    }

    public static SubtaskExecutor newInstance(SubtaskMaster subtaskMaster) {
        SubtaskExecutor executor = uninitializedNewInstance(subtaskMaster);
        executor.initialize();
        return executor;
    }

    // Broken out for unit testing.
    static SubtaskExecutor uninitializedNewInstance(SubtaskMaster subtaskMaster) {
        for (SubtaskExecutorType subtaskExecutorType : SubtaskExecutorType.values()) {
            SubtaskExecutor executor = subtaskExecutorType.subtaskExecutor(subtaskMaster);
            if (executor != null) {
                return executor;
            }
        }
        return new SubtaskExecutor(subtaskMaster.taskDir(), subtaskMaster.getSubtaskIndex(),
            subtaskMaster.getBinaryName(), subtaskMaster.getTimeoutSecs(),
            subtaskMaster.getHeapSizeGigabytes(), subtaskMaster.getTaskConfiguration());
    }
}
