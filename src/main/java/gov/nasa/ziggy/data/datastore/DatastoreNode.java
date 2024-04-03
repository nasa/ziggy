package gov.nasa.ziggy.data.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlTransient;

@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "Ziggy_DatastoreNode",
    uniqueConstraints = { @UniqueConstraint(columnNames = { "fullPath" }) })
public class DatastoreNode {

    public static final String CHILD_NODE_NAME_DELIMITER = ",";

    @XmlTransient
    @Transient
    private static final Logger log = LoggerFactory.getLogger(DatastoreNode.class);

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_DatastoreNode_generator")
    @SequenceGenerator(name = "ziggy_DatastoreNode_generator", initialValue = 1,
        sequenceName = "ziggy_DatastoreNode_sequence", allocationSize = 1)
    private Long id;

    @XmlAttribute(required = true)
    private String name;

    // The full path, relative to datastore root, for the node.
    @XmlTransient
    private String fullPath = "";

    // Indicates that this node is a representation of a regular expression object.
    @XmlAttribute(required = false, name = "isRegexp")
    private Boolean regexp = false;

    // The names of the child nodes to this node. XML only.
    @Transient
    @XmlAttribute(required = false)
    private String nodes = "";

    // Full paths to the child nodes to this node. Database only.
    // Note that each node always needs to know its child nodes,
    // hence fetch type here is EAGER.
    @XmlTransient
    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_DatastoreNode_childNodeFullPaths")
    private List<String> childNodeFullPaths = new ArrayList<>();

    // Nodes that are elements of the current node. XML only.
    @Transient
    @XmlElement(name = "datastoreNode", required = false)
    private List<DatastoreNode> xmlNodes = new ArrayList<>();

    public DatastoreNode() {
    }

    /** For testing only. */
    DatastoreNode(String name, boolean regexp) {
        this.name = name;
        this.regexp = regexp;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public String getFullPath() {
        return fullPath;
    }

    public Boolean isRegexp() {
        return regexp;
    }

    /**
     * Package scoped because only {@link DatastoreConfigurationImporter} should be able to modify
     * this.
     */
    void setRegexp(Boolean regexp) {
        this.regexp = regexp;
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public List<String> getChildNodeFullPaths() {
        return childNodeFullPaths;
    }

    public void setChildNodeFullPaths(List<String> childNodeFullPaths) {
        this.childNodeFullPaths = childNodeFullPaths;
    }

    public List<DatastoreNode> getXmlNodes() {
        return xmlNodes;
    }

    public void setXmlNodes(List<DatastoreNode> xmlNodes) {
        this.xmlNodes = xmlNodes;
    }

    // Given that fullPath has to be unique in the database, it's an acceptable field to use
    // for hashCode() and equals().
    @Override
    public int hashCode() {
        return Objects.hash(fullPath);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DatastoreNode other = (DatastoreNode) obj;
        return Objects.equals(fullPath, other.fullPath);
    }
}
