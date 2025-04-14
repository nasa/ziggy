/**
 * Provides classes and interfaces for executing data analysis algorithms on specified chunks of
 * data. Algorithms will typically use the
 * {@link gov.nasa.ziggy.pipeline.step.AlgorithmPipelineStepExecutor} class to manage the steps of
 * execution (marshaling, submitting, executing, persisting, etc.), and will use
 * {@link gov.nasa.ziggy.pipeline.step.io.PipelineInputs} and
 * {@link gov.nasa.ziggy.pipeline.step.io.PipelineOutputs} to hold information about task-level and
 * subtask-level inputs and outputs for the algorithm processes.
 * <h2>Processing Steps</h2>
 * <p>
 * The processing steps found in the {@link gov.nasa.ziggy.pipeline.definition.ProcessingStep} enum
 * are set with the
 * {@link gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations#updateProcessingStep(gov.nasa.ziggy.pipeline.definition.PipelineTask, gov.nasa.ziggy.pipeline.definition.ProcessingStep)}
 * method and implicitly with the
 * {@link gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor#incrementProcessingStep()} method.
 * Here is where each of these steps occur:
 * <dl>
 * <dt>INITIALIZING</dt>
 * <dd>Set when a {@link gov.nasa.ziggy.pipeline.definition.PipelineTask} is created.</dd>
 * <dt>WAITING_TO_RUN</dt>
 * <dd>Set in the {@link gov.nasa.ziggy.pipeline.PipelineExecutor}.</dd>
 * <dt>MARSHALING</dt>
 * <dd>Set implicitly in the {@link gov.nasa.ziggy.worker.TaskExecutor} in the worker.</dd>
 * <dt>SUBMITTING</dt>
 * <dd>Set in the {@link gov.nasa.ziggy.pipeline.step.AlgorithmPipelineStepExecutor}.</dd>
 * <dt>QUEUED</dt>
 * <dd>Set by the {@link gov.nasa.ziggy.pipeline.step.remote.RemoteAlgorithmExecutor}.</dd>
 * <dt>EXECUTING and WAITING_TO_STORE</dt>
 * <dd>Set by {@link gov.nasa.ziggy.pipeline.step.AlgorithmMonitor}.</dd>
 * <dt>STORING</dt>
 * <dd>Set implicitly in the
 * {@link gov.nasa.ziggy.pipeline.step.AlgorithmPipelineStepExecutor}.</dd>
 * <dt>COMPLETE</dt>
 * <dd>Set in the {@link gov.nasa.ziggy.worker.TaskExecutor} in the worker.</dd>
 * </dl>
 *
 * @author Bill Wohler
 * @author PT
 */

package gov.nasa.ziggy.pipeline.step;
