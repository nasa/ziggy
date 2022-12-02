package gov.nasa.ziggy.services.events;

import java.util.Date;
import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionName;

/**
 * Permanent record of a Ziggy event for storage in the database.
 *
 * @author PT
 */
@Entity
@Table(name = "PI_ZIGGY_EVENT")
public class ZiggyEvent {

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ZiggyEvent other = (ZiggyEvent) obj;
        return id == other.id;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_EVENT_SEQ",
        allocationSize = 1)
    private long id;

    private String eventHandlerName;

    @ManyToOne(targetEntity = PipelineDefinitionName.class, fetch = FetchType.EAGER)
    private PipelineDefinitionName pipelineName;
    private Date eventTime;
    private long pipelineInstanceId;

    @SuppressWarnings("unused")
    private ZiggyEvent() {
    }

    public ZiggyEvent(String eventHandlerName, PipelineDefinitionName pipelineName,
        long pipelineInstance) {
        this.eventHandlerName = eventHandlerName;
        this.pipelineName = pipelineName;
        eventTime = new Date();
        pipelineInstanceId = pipelineInstance;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getEventHandlerName() {
        return eventHandlerName;
    }

    public void setEventHandlerName(String eventHandlerName) {
        this.eventHandlerName = eventHandlerName;
    }

    public PipelineDefinitionName getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(PipelineDefinitionName pipelineName) {
        this.pipelineName = pipelineName;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }

    public void setPipelineInstanceId(long pipelineInstance) {
        pipelineInstanceId = pipelineInstance;
    }

}
