package gov.nasa.ziggy.data.datastore;

import java.io.Serializable;
import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;

/**
 * Defines a data file type for a pipeline. Data file types are used as input or output file types
 * for each pipeline module.
 * <p>
 * Data file types are defined by a location in the datastore, where the location is defined as:
 *
 * <pre>
 * locationElement1/locationElement2/locationElement3.../locationElementN,
 * </pre>
 *
 * where each locationElement contains the name of a {@link DatastoreNode} instance. If the instance
 * is a reference to a {@link DatastoreRegexp}, then the locationElement can also contain a string
 * to be used with the regexp, separated from the node name by a $ (i.e., if the regexp is named
 * cadenceType and has valid values of "(target|ffi)", then the locationElement would be either
 * "cadenceType$target" or "cadenceType$ffi"). The full path defined by the locatiionElement
 * elements must correspond to the full path of a datastore node in the database.
 * <p>
 * Ziggy uses the location of a {@link DataFileType} to identify all the directories that
 * potentially have data that can be used in processing in a particular module, or to find the
 * destination of any output files from a given pipeline module.
 * <p>
 * The {@link DataFileType} also requires a String that is a regular expression for the data file
 * names that correspond to this data file type.
 * <p>
 * Each {@link DataFileType} instance has a Boolean field,
 * {@link DataFileType#includeAllFilesInAllSubtasks}. This indicates whether the data file type will
 * provide 1 file of the given type to each subtask (default value, false) or whether the data file
 * type will provide each subtask with all the files in that type (true). The field is a Boolean
 * rather than boolean because it corresponds to an optional XML attribute, which means that it
 * cannot be a primitive type.
 * <p>
 * The XML definition of a {@link DataFileType} allows the user to specify datastore nodes in the
 * {@link #fileNameRegexp}, i.e., the user can specify a fileNameRegexp of
 * "foo/bar/file-[0-9]{2}.txt" in the fileNameRegexp attribute. This allows the user to design a
 * system in which the unit of work (determined by the location) can contain multiple directories
 * that must be searched for input files or populated with output files.
 *
 * @author PT
 */

@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "ziggy_DataFileType")
public class DataFileType implements Serializable {

    private static final long serialVersionUID = 20240122L;

    @Id
    @XmlAttribute(required = true)
    private String name;

    @XmlAttribute(required = true)
    private String location;

    @XmlAttribute(required = true)
    private String fileNameRegexp;

    @XmlAttribute(required = false)
    private Boolean includeAllFilesInAllSubtasks = false;

    /**
     * "The JPA specification requires all Entity classes to have a default no-arg constructor. This
     * can be either public or protected."
     */
    protected DataFileType() {
    }

    /** For testing only. */
    public DataFileType(String name, String location, String fileNameRegexp) {
        this.name = name;
        this.location = location;
        this.fileNameRegexp = fileNameRegexp;
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public String getFileNameRegexp() {
        return fileNameRegexp;
    }

    public boolean isIncludeAllFilesInAllSubtasks() {
        return includeAllFilesInAllSubtasks != null ? includeAllFilesInAllSubtasks : false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileNameRegexp, location, name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DataFileType other = (DataFileType) obj;
        return Objects.equals(fileNameRegexp, other.fileNameRegexp)
            && Objects.equals(location, other.location) && Objects.equals(name, other.name);
    }
}
