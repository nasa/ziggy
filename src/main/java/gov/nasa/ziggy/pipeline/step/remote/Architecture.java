package gov.nasa.ziggy.pipeline.step.remote;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.ui.pipeline.RemoteExecutionDialog;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlTransient;

/** Models a compute node architecture for a remote execution environment. */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_architecture")
public class Architecture {

    @XmlTransient
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_Architecture_generator")
    @SequenceGenerator(name = "ziggy_architecture_generator", initialValue = 1,
        sequenceName = "ziggy_architecture_sequence", allocationSize = 1)
    private Long id;

    /** Name used by the batch system. */
    @XmlAttribute(required = true)
    private String name;

    /** More useful information displayed by {@link RemoteExecutionDialog}. */
    @XmlAttribute(required = true)
    private String description;

    /** Cores per node. */
    @XmlAttribute(required = true)
    private int cores;

    /** Ram per node in GB. */
    @XmlAttribute(required = true)
    private int ramGigabytes;

    /** Cost per node per hour of use. */
    @XmlAttribute(required = true)
    private float cost;

    /** Bandwidth of network connection in Gbps. */
    @XmlAttribute(required = false)
    private Float bandwidthGbps;

    /** Name of the file with the list of node names. Optional. */
    @XmlAttribute(required = false)
    private String nodeCollectionNamesFile;

    public Architecture() {
    }

    public void updateFrom(Architecture importedArchitecture) {
        description = importedArchitecture.description;
        cores = importedArchitecture.cores;
        ramGigabytes = importedArchitecture.ramGigabytes;
        cost = importedArchitecture.cost;
        bandwidthGbps = importedArchitecture.bandwidthGbps;
    }

    public float gigsPerCore() {
        return (float) getRamGigabytes() / (float) getCores();
    }

    public boolean hasSufficientRam(double requiredRamGigabytes) {
        return getRamGigabytes() >= requiredRamGigabytes;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public int getCores() {
        return cores;
    }

    public int getRamGigabytes() {
        return ramGigabytes;
    }

    public float getCost() {
        return cost;
    }

    public float getBandwidthGbps() {
        return bandwidthGbps != null ? bandwidthGbps : 0;
    }

    public String getNodeCollectionNamesFile() {
        return nodeCollectionNamesFile;
    }

    public Path expandedNodeCollectionNamesFile() {
        return StringUtils.isBlank(nodeCollectionNamesFile) ? null
            : Paths.get((String) ZiggyConfiguration.interpolate(nodeCollectionNamesFile));
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Architecture other = (Architecture) obj;
        return Objects.equals(name, other.name);
    }
}
