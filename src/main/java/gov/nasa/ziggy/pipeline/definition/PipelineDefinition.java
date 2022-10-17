package gov.nasa.ziggy.pipeline.definition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import javax.persistence.CascadeType;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderColumn;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.Version;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.hibernate.annotations.Cascade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.parameters.Parameters;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.util.CollectionFilters;

/**
 * This class a pipeline configuration. A 'pipeline' is defined as a directed graph of
 * {@link PipelineDefinitionNode}s that share a common unit of work definition.
 *
 * @author Todd Klaus
 */
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_PIPELINE_DEF")
public class PipelineDefinition implements CanBeDeclaredObsolete {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineDefinition.class);

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_PD_SEQ",
        allocationSize = 1)
    private long id;

    @Embedded
    // init with empty placeholder, to be filled in by console
    private AuditInfo auditInfo = new AuditInfo();

    /**
     * used by Hibernate to implement optimistic locking. Should prevent 2 different console users
     * from clobbering each others changes
     */
    @Version
    private int dirty = 0;

    @XmlAttribute(required = true)
    private String description = null;

    // Combination of name+version must be unique (see shared-extra-ddl-create.sql)
    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(PipelineDefinitionName.PipelineNameAdapter.class)
    @ManyToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    private PipelineDefinitionName name;

    private int version = 0;

    @ManyToOne
    private Group group = null;

    // Using the Integer class rather than int here because XML won't allow optional
    // attributes that are primitive types
    @XmlAttribute(required = false)
    private Integer instancePriority = PipelineInstance.LOWEST_PRIORITY;

    /**
     * {@link ParameterSetName}s that will be used as {@link Parameters} when an instance of this
     * pipeline is launched. At launch-time, the {@Link PipelineInstance} will be given a
     * hard-reference to a specific version (typically latest) of the {@link ParameterSet} for this
     * {@link ParameterSetName}
     */
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(name = "PI_TD_PSN")
    private Map<ClassWrapper<Parameters>, ParameterSetName> pipelineParameterSetNames = new HashMap<>();

    /**
     * Set to true when the first pipeline instance is created using this definition in order to
     * preserve the data accountability record. Editing a locked definition will result in a new,
     * unlocked instance with the version incremented
     */
    private boolean locked = false;

    @OneToMany
    @Cascade({ org.hibernate.annotations.CascadeType.SAVE_UPDATE,
        org.hibernate.annotations.CascadeType.DETACH })
    @JoinTable(name = "PI_PDN_ROOT_NODES", joinColumns = @JoinColumn(name = "PARENT_PI_PD_ID"),
        inverseJoinColumns = @JoinColumn(name = "CHILD_PI_PDN_ID"))
    @OrderColumn(name = "IDX")
    private List<PipelineDefinitionNode> rootNodes = new ArrayList<>();

    // Root node names are stored until post-processing can connect them to
    // the names of nodes in the pipeline.
    @XmlAttribute(required = true)
    @Transient
    private String rootNodeNames;

    // The following construction makes it possible to put the nodes and parameter sets for a
    // pipeline in any order in the XML, but it also complicates the bookkeeping required for those
    // objects.
    @XmlElements({ @XmlElement(name = "node", type = PipelineDefinitionNode.class),
        @XmlElement(name = "pipelineParameter", type = ParameterSetName.class) })
    @Transient
    private Set<Object> nodesAndParamSets = new HashSet<>();

    public PipelineDefinition() {
    }

    public PipelineDefinition(String name) {
        this.name = new PipelineDefinitionName(name);
    }

    public PipelineDefinition(AuditInfo auditInfo, String name) {
        this.name = new PipelineDefinitionName(name);
        this.auditInfo = auditInfo;
    }

    /**
     * Copy constructor
     *
     * @param other
     */
    public PipelineDefinition(PipelineDefinition other) {
        name = other.name;
        description = other.description;
        auditInfo = other.auditInfo;
        version = 0;
        group = other.group;
        locked = false;

        for (PipelineDefinitionNode otherNode : other.rootNodes) {
            rootNodes.add(new PipelineDefinitionNode(otherNode));
        }
    }

    @Override
    public void rename(String name) {
        this.name = new PipelineDefinitionName(name);
    }

    /**
     * Creates a new, unlocked version of this {@link PipelineDefinition} Called by the console when
     * the user edits a locked instance. If the instance is unlocked, the instance itself will be
     * returned.
     */
    public PipelineDefinition newVersion() {
        return newVersion(false);
    }

    /**
     * Creates a new, unlocked version of this {@link PipelineDefinition} that has had its content
     * replaced by the content of another {@link PipelineDefinition} instance. If the instance is
     * unlocked, the instance itself will be returned.
     */
    public PipelineDefinition newVersionWithUpdatedContent(PipelineDefinition contentSource) {
        PipelineDefinition newVersion = newVersion(true);
        newVersion.description = contentSource.description;
        newVersion.instancePriority = contentSource.instancePriority;
        newVersion.rootNodeNames = contentSource.rootNodeNames;
        newVersion.nodesAndParamSets = contentSource.nodesAndParamSets;
        return newVersion;
    }

    private PipelineDefinition newVersion(boolean clearNodesAndParams) throws PipelineException {
        PipelineDefinition newVersion = this;
        if (locked) {
            newVersion = new PipelineDefinition(this);
            newVersion.version = version + 1;
        }

        if (clearNodesAndParams) {
            newVersion.pipelineParameterSetNames.clear();
            newVersion.rootNodes.clear();
        }
        return newVersion;
    }

    /**
     * Sets the state of this {@link PipelineDefinition}, and all associated
     * {@link PipelineDefinitionNode}s, {@link PipelineModuleDefinition}s, and
     * {@link ParameterSet}s.
     * <p>
     * Normally called by the {@link PipelineExecutor} when a pipeline instance is launched in order
     * to preserve the data accountability record by preventing these definitions from being
     * modified once they are used.
     *
     * @throws PipelineException
     */
    public void lock() throws PipelineException {
        locked = true;
    }

    /**
     * Walks the tree of {@link PipelineDefinitionNode}s for this pipeline definition and sets the
     * parentNode and path fields for each one.
     */
    public void buildPaths() {
        Stack<Integer> path = new Stack<>();
        buildPaths(null, rootNodes, path);
    }

    /**
     * Recursive method to set parent pointers
     *
     * @param parent
     * @param kids
     * @param path
     */
    private void buildPaths(PipelineDefinitionNode parent, List<PipelineDefinitionNode> kids,
        Stack<Integer> path) {
        for (int i = 0; i < kids.size(); i++) {
            PipelineDefinitionNode kid = kids.get(i);
            path.push(i);

            kid.setParentNode(parent);
            kid.setPath(new PipelineDefinitionNodePath(new ArrayList<>(path)));

            buildPaths(kid, kid.getNextNodes(), path);

            path.pop();
        }
    }

    public long getId() {
        return id;
    }

    public PipelineDefinitionName getName() {
        return name;
    }

    /**
     * @return Returns the locked.
     */
    public boolean isLocked() {
        return locked;
    }

    @Override
    public String toString() {
        return name.toString();
    }

    public List<PipelineDefinitionNode> getRootNodes() {
        return rootNodes;
    }

    public void setRootNodes(List<PipelineDefinitionNode> rootNodes) {
        this.rootNodes = rootNodes;
        CollectionFilters.removeTypeFromCollection(nodesAndParamSets, PipelineDefinitionNode.class);
        populateXmlFields();
    }

    // Set the root node names from the rootNodes field.
    private void setRootNodeNames() {
        if (rootNodes.isEmpty()) {
            rootNodeNames = null;
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (PipelineDefinitionNode node : rootNodes) {
            sb.append(node.getModuleName().getName());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        rootNodeNames = sb.toString();
    }

    public void addRootNode(PipelineDefinitionNode rootNode) {
        rootNodes.add(rootNode);
        populateXmlFields();
    }

    /**
     * Populates the fields that are used by the XML generator (specifically, the
     * {@link #nodesAndParamSets} and {@link #rootNodeNames} fields) from the other fields in the
     * instance (specifically, the ones that are obtained from a database retrieval). This ensures
     * that the database and XML versions of the instance's information are consistent with one
     * another at the moment of retrieval. The method also calls the
     * {@link PipelineDefinitionNode#populateXmlFields()} method for all nodes in the pipeline, thus
     * ensuring that they are also self-consistent.
     * <p>
     * Note: This method must be called whenever any of the set or add methods for nodes or
     * parameter sets are invoked. This ensures that the {@link #nodesAndParamSets} field remains
     * synchronized to the database fields that it mirrors.
     * <p>
     * Note: when any method sets the value of a database field that is mirrored in the
     * {@link #nodesAndParamSets} field, the {@link #nodesAndParamSets} field must have all
     * instances of the appropriate object class removed via the
     * {@link CollectionFilters#removeTypeFromCollection(java.util.Collection, Class)} static
     * method. This is necessary to ensure that (a) the {@link #nodesAndParamSets} field doesn't
     * keep any instances that are now obsolete due to the set operation, but (b) the instances of
     * all other classes in the {@link #nodesAndParamSets} field are not removed when the instances
     * from the given class are removed.
     */
    public void populateXmlFields() {
        List<PipelineDefinitionNode> allNodes = getNodes();
        nodesAndParamSets.addAll(allNodes);
        for (PipelineDefinitionNode node : allNodes) {
            node.populateXmlFields();
        }
        nodesAndParamSets.addAll(pipelineParameterSetNames.values());

        // Don't touch the rootNodeNames String unless the rootNodes List is populated
        if (!rootNodes.isEmpty()) {
            setRootNodeNames();
        }
    }

    public AuditInfo getAuditInfo() {
        return auditInfo;
    }

    public void setAuditInfo(AuditInfo auditInfo) {
        this.auditInfo = auditInfo;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public Group getGroup() {
        return group;
    }

    @Override
    public void setGroup(Group group) {
        this.group = group;
    }

    public int getDirty() {
        return dirty;
    }

    /**
     * For TEST USE ONLY
     */
    public void setDirty(int dirty) {
        this.dirty = dirty;
    }

    @Override
    public String getGroupName() {
        Group group = getGroup();
        if (group == null) {
            return Group.DEFAULT_GROUP.getName();
        } else {
            return group.getName();
        }
    }

    @Override
    public String getDatabaseName() {
        return getName().getName();
    }

    public int getInstancePriority() {
        return instancePriority;
    }

    public void setInstancePriority(int instancePriority) {
        this.instancePriority = instancePriority;
    }

    public Map<ClassWrapper<Parameters>, ParameterSetName> getPipelineParameterSetNames() {
        return pipelineParameterSetNames;
    }

    // Populates the Map from Parameter class to ParameterSetName.
    public void setPipelineParameterSetNames(
        Map<ClassWrapper<Parameters>, ParameterSetName> pipelineParameterSetNames) {
        this.pipelineParameterSetNames = pipelineParameterSetNames;
        CollectionFilters.removeTypeFromCollection(nodesAndParamSets, ParameterSetName.class);
        populateXmlFields();
    }

    // Adds an element to the Map from Parameter class to ParameterSetName.
    public void addPipelineParameterSetName(Class<? extends Parameters> parameterClass,
        ParameterSet parameterSet) {
        ClassWrapper<Parameters> classWrapper = new ClassWrapper<>(parameterClass);
        if (pipelineParameterSetNames.containsKey(classWrapper)) {
            throw new PipelineException(
                "This PipelineDefinition already contains a pipeline parameter set for class: "
                    + parameterClass);
        }
        pipelineParameterSetNames.put(classWrapper, parameterSet.getName());
        populateXmlFields();
    }

    // Obtains all the nodes by walking the tree from the root nodes.
    public List<PipelineDefinitionNode> getNodes() {
        List<PipelineDefinitionNode> pipelineDefinitionNodes = new ArrayList<>();
        addNodesToList(pipelineDefinitionNodes, rootNodes);
        return pipelineDefinitionNodes;
    }

    private void addNodesToList(List<PipelineDefinitionNode> pipelineDefinitionNodes,
        List<PipelineDefinitionNode> newNodes) {
        for (PipelineDefinitionNode definitionNode : newNodes) {
            if (pipelineDefinitionNodes.contains(definitionNode)) {
                throw new PipelineException(
                    "Circular reference: pipelineNodes Set already contains this node: "
                        + definitionNode);
            }
            pipelineDefinitionNodes.add(definitionNode);
            addNodesToList(pipelineDefinitionNodes, definitionNode.getNextNodes());
        }
    }

    public PipelineDefinitionNode getNodeByName(String name) {
        if (name == null) {
            return null;
        }
        List<PipelineDefinitionNode> nodes = getNodes();
        List<PipelineDefinitionNode> nodeNameMatches = nodes.stream()
            .filter(s -> name.equals(s.getModuleName().getName()))
            .collect(Collectors.toList());
        PipelineDefinitionNode matchedNode = null;
        if (!nodeNameMatches.isEmpty()) {
            matchedNode = nodeNameMatches.get(0);
        }
        return matchedNode;
    }

    public Set<PipelineDefinitionNode> getNodesFromXml() {
        return CollectionFilters.filterToSet(nodesAndParamSets, PipelineDefinitionNode.class);
    }

    public Set<ParameterSetName> getParameterSetNames() {
        return CollectionFilters.filterToSet(nodesAndParamSets, ParameterSetName.class);
    }

    public String getRootNodeNames() {
        return rootNodeNames;
    }

    // Set the root nodes from a String.
    public void setRootNodeNames(String rootNodeNames) {
        this.rootNodeNames = rootNodeNames;
    }

}
