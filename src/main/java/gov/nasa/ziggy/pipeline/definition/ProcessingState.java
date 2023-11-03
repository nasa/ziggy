package gov.nasa.ziggy.pipeline.definition;

/**
 * Sub-state when state == PROCESSING The purpose of this field is to provide more fine-grained
 * status for the pipeline operator. The enumerations also provide support for actions that are to
 * be performed in each state; these actions are defined by an implementation of
 * {@link ProcessingStatePipelineModule}.
 */
public enum ProcessingState {
    INITIALIZING {
        @Override
        public String shortName() {
            return "I";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.initializingTaskAction();
        }
    },
    MARSHALING {
        @Override
        public String shortName() {
            return "M";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.marshalingTaskAction();
        }
    },
    ALGORITHM_EXECUTING {
        @Override
        public String shortName() {
            return "Ae";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.executingTaskAction();
        }
    },
    STORING {
        @Override
        public String shortName() {
            return "S";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.storingTaskAction();
        }
    },
    COMPLETE {
        @Override
        public String shortName() {
            return "C";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.processingCompleteTaskAction();
        }
    },
    ALGORITHM_SUBMITTING {
        @Override
        public String shortName() {
            return "As";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.submittingTaskAction();
        }
    },
    ALGORITHM_QUEUED {
        @Override
        public String shortName() {
            return "Aq";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.queuedTaskAction();
        }
    },
    ALGORITHM_COMPLETE {
        @Override
        public String shortName() {
            return "Ac";
        }

        @Override
        public void taskAction(ProcessingStatePipelineModule module) {
            module.algorithmCompleteTaskAction();
        }
    };

    public abstract String shortName();

    public abstract void taskAction(ProcessingStatePipelineModule module);
}
