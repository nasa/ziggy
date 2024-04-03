package gov.nasa.ziggy.services.events;

import java.util.Date;
import java.util.Objects;
import java.util.Set;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Permanent record of a Ziggy event for storage in the database.
 *
 * @author PT
 */
@Entity
@Table(name = "ziggy_Event")
public class ZiggyEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_Event_generator")
    @SequenceGenerator(name = "ziggy_Event_generator", initialValue = 1,
        sequenceName = "ziggy_Event_sequence", allocationSize = 1)
    private Long id;

    private String eventHandlerName;

    private String pipelineName;
    private Date eventTime;
    private long pipelineInstanceId;

    @ElementCollection
    @JoinTable(name = "ziggy_Event_eventLabels")
    private Set<String> eventLabels;

    @SuppressWarnings("unused")
    private ZiggyEvent() {
    }

    public ZiggyEvent(String eventHandlerName, String pipelineName, long pipelineInstance,
        Set<String> eventLabels) {
        this.eventHandlerName = eventHandlerName;
        this.pipelineName = pipelineName;
        this.eventLabels = eventLabels;
        eventTime = new Date();
        pipelineInstanceId = pipelineInstance;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEventHandlerName() {
        return eventHandlerName;
    }

    public void setEventHandlerName(String eventHandlerName) {
        this.eventHandlerName = eventHandlerName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
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

    public Set<String> getEventLabels() {
        return eventLabels;
    }

    public void setEventLabels(Set<String> eventLabels) {
        this.eventLabels = eventLabels;
    }

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
        return Objects.equals(id, other.id);
    }
}
