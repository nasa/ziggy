package gov.nasa.ziggy.ui.mon.master;

public class PipelineInstanceIndicator extends Indicator {
    private static final long serialVersionUID = 573620631554800492L;

    private final LabelValue idLV = new LabelValue("id", "0");
    private final LabelValue pipelineLV = new LabelValue("pipeline", "PA");
    private final LabelValue stateLV = new LabelValue("state", "PROCESSING");
    private final LabelValue tasksLV = new LabelValue("tasks", "140/5/20/3");
    private final LabelValue workersLV = new LabelValue("workers", "5");

    public PipelineInstanceIndicator(IndicatorPanel parentIndicatorPanel, String name) {
        super(parentIndicatorPanel, name);
        setPreferredSize(new java.awt.Dimension(220, 65));

        // addDataComponent( idLV );
        addDataComponent(pipelineLV);
        addDataComponent(stateLV);
        addDataComponent(tasksLV);
        addDataComponent(workersLV);
    }

    /**
     * @return Returns the id.
     */
    @Override
    public String getId() {
        return idLV.getValue();
    }

    /**
     * @param id The id to set.
     */
    @Override
    public void setId(String id) {
        idLV.setValue(id);
        setIndicatorDisplayName(id);
    }

    /**
     * @return Returns the pipeline.
     */
    public String getPipeline() {
        return pipelineLV.getValue();
    }

    /**
     * @param pipeline The pipeline to set.
     */
    public void setPipeline(String pipeline) {
        pipelineLV.setValue(pipeline);
    }

    /**
     * @return Returns the state.
     */
    public String getLVState() {
        return stateLV.getValue();
    }

    /**
     * @param state The state to set.
     */
    public void setState(String state) {
        stateLV.setValue(state);
    }

    /**
     * @return Returns the tasks.
     */
    public String getTasks() {
        return tasksLV.getValue();
    }

    /**
     * @param tasks The tasks to set.
     */
    public void setTasks(String tasks) {
        tasksLV.setValue(tasks);
    }

    /**
     * @return Returns the workers.
     */
    public String getWorkers() {
        return workersLV.getValue();
    }

    /**
     * @param workers The workers to set.
     */
    public void setWorkers(String workers) {
        workersLV.setValue(workers);
    }
}
