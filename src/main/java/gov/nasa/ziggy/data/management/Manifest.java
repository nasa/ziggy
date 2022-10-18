/*
 * Copyright (C) 2022 United States Government as represented by the Administrator of the National
 * Aeronautics and Space Administration. All Rights Reserved.
 *
 * NASA acknowledges the SETI Institute's primary role in authoring and producing Ziggy, a Pipeline
 * Management System for Data Analysis Pipelines, under Cooperative Agreement Nos. NNX14AH97A,
 * 80NSSC18M0068 & 80NSSC21M0079.
 *
 * This file is available under the terms of the NASA Open Source Agreement (NOSA). You should have
 * received a copy of this agreement with the Ziggy source code; see the file LICENSE.pdf.
 *
 * Disclaimers
 *
 * No Warranty: THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY KIND, EITHER
 * EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY THAT THE SUBJECT
 * SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL BE
 * ERROR FREE, OR ANY WARRANTY THAT DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT
 * SOFTWARE. THIS AGREEMENT DOES NOT, IN ANY MANNER, CONSTITUTE AN ENDORSEMENT BY GOVERNMENT AGENCY
 * OR ANY PRIOR RECIPIENT OF ANY RESULTS, RESULTING DESIGNS, HARDWARE, SOFTWARE PRODUCTS OR ANY
 * OTHER APPLICATIONS RESULTING FROM USE OF THE SUBJECT SOFTWARE. FURTHER, GOVERNMENT AGENCY
 * DISCLAIMS ALL WARRANTIES AND LIABILITIES REGARDING THIRD-PARTY SOFTWARE, IF PRESENT IN THE
 * ORIGINAL SOFTWARE, AND DISTRIBUTES IT "AS IS."
 *
 * Waiver and Indemnity: RECIPIENT AGREES TO WAIVE ANY AND ALL CLAIMS AGAINST THE UNITED STATES
 * GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT. IF RECIPIENT'S
 * USE OF THE SUBJECT SOFTWARE RESULTS IN ANY LIABILITIES, DEMANDS, DAMAGES, EXPENSES OR LOSSES
 * ARISING FROM SUCH USE, INCLUDING ANY DAMAGES FROM PRODUCTS BASED ON, OR RESULTING FROM,
 * RECIPIENT'S USE OF THE SUBJECT SOFTWARE, RECIPIENT SHALL INDEMNIFY AND HOLD HARMLESS THE UNITED
 * STATES GOVERNMENT, ITS CONTRACTORS AND SUBCONTRACTORS, AS WELL AS ANY PRIOR RECIPIENT, TO THE
 * EXTENT PERMITTED BY LAW. RECIPIENT'S SOLE REMEDY FOR ANY SUCH MATTER SHALL BE THE IMMEDIATE,
 * UNILATERAL TERMINATION OF THIS AGREEMENT.
 */

package gov.nasa.ziggy.data.management;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Transient;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.xml.sax.SAXException;

import gov.nasa.ziggy.data.management.Acknowledgement.AcknowledgementEntry;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.io.FileUtil;

/**
 * Models a data receipt manifest. A manifest is an XML file that specifies the name, size, and
 * checksum of each file in a given data transfer. This is used to validate the transfer during data
 * receipt. Once the data receipt process has completed, an acknowledgement file is generated that
 * summarizes the state of the transfer.
 * <p>
 * The manifest class contains fields that are used in the XML file and fields that are used in the
 * database table of manifests for data receipt activities, but this information is sadly disjoint
 * between these two use-cases. Notwithstanding this, both sets of fields are managed by this class
 * in the interest of sanity.
 * <p>
 * Manifests written to XML files must have "-manifest.xml" as the end of the filename. Manifests
 * can be constructed using regular files or symlinks as the files that are represented in the
 * manifest. If the files are symlinks, then the true source file will be used for the file size and
 * the checksum, but the symlink will be used for the file name.
 *
 * @see Acknowledgement
 * @author PT
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
@Entity
@Table(name = "PI_MANIFEST")
public class Manifest implements HasXmlSchemaFilename {

    private static final String SCHEMA_FILENAME = "manifest.xsd";
    static final String FILENAME_SUFFIX = "-manifest.xml";

    // Thread pool for checksum calculations
    static ExecutorService checksumThreadPool = Executors
        .newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    // Add a shutdown hook for the checksum thread pool
    static {
        ZiggyShutdownHook.addShutdownHook(() -> checksumThreadPool.shutdownNow());
    }

    /**
     * Defines the checksum type used for both {@link Manifest} and {@link Acknowledgement}
     * instances.
     */
    public static final ChecksumType CHECKSUM_TYPE = ChecksumType.SHA1;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_MANIFEST_SEQ",
        allocationSize = 1)
    private long id;

    @Column
    @XmlAttribute(required = true)
    private long datasetId;

    @Transient
    @XmlAttribute(required = true)
    private final ChecksumType checksumType = CHECKSUM_TYPE;

    @Column
    private String name = "";

    @Transient
    @XmlAttribute(required = true)
    private int fileCount;

    @Column
    private Date importTime;

    @Column
    private long importTaskId;

    @Column
    @Enumerated(EnumType.STRING)
    private DataReceiptStatus status;

    @Column
    private boolean acknowledged;

    @Transient
    @XmlElement(required = true, name = "file")
    private List<ManifestEntry> manifestEntries = new ArrayList<>();

    @Override
    public String getXmlSchemaFilename() {
        return SCHEMA_FILENAME;
    }

    public Map<String, ManifestEntry> fileNameToManifestEntry() {
        Map<String, ManifestEntry> fileNameToManifestFile = new HashMap<>();
        for (ManifestEntry entry : manifestEntries) {
            fileNameToManifestFile.put(entry.getName(), entry);
        }
        return fileNameToManifestFile;
    }

    /**
     * Generates a {@link Manifest} from the contents of a directory. The manifest will include all
     * regular files or symlinks to regular files, including files that are in subdirectories of the
     * directory provided as argument.
     */
    public static Manifest generateManifest(Path directory, long datasetId) throws IOException {

        Manifest manifest = new Manifest();
        manifest.setDatasetId(datasetId);
        List<Future<ManifestEntry>> futures = new ArrayList<>();

        // Get all the files to include in the manifest. Some of these may be symbolic links.
        Map<Path, Path> regularFiles = FileUtil.regularFilesInDirTree(directory);

        // Create a new ManifestEntry for each file. In the interest of performance this is
        // done using a thread pool, with each ManifestEntry generated in a separate thread.
        // This maximizes the throughput of the checksum calculations.
        for (Map.Entry<Path, Path> regularFileMapEntry : regularFiles.entrySet()) {
            futures.add(checksumThreadPool.submit(() -> {
                ManifestEntry manifestEntry = new ManifestEntry();
                manifestEntry.setName(regularFileMapEntry.getKey().toString());
                Path realFile = regularFileMapEntry.getValue();
                try {
                    manifestEntry.setSize(Files.size(realFile));
                    manifestEntry.setChecksum(manifest.checksumType.checksum(realFile));
                } catch (IOException e) {
                    throw new PipelineException("Unable to calculate checksum or file size "
                        + " of file " + manifestEntry.getName(), e);
                }
                return manifestEntry;
            }));
        }

        for (Future<?> future : futures) {
            try {
                manifest.getManifestEntries().add((ManifestEntry) future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new PipelineException("Exception occurred waiting for checksum results", e);
            }
        }
        manifest.setFileCount(manifest.getManifestEntries().size());
        return manifest;
    }

    /**
     * Writes the manifest to the specified directory. The manifest name must be set to a valid
     * value; if not, a {@link PipelineException} will occur.
     */
    public void write(Path directory)
        throws InstantiationException, IllegalAccessException, SAXException, JAXBException {

        validateName(name);
        ValidatingXmlManager<Manifest> xmlManager = new ValidatingXmlManager<>(Manifest.class);
        directory.toFile().mkdirs();

        Path fullPath = directory.resolve(name);
        xmlManager.marshal(this, fullPath.toFile());
    }

    public static Manifest readManifest(Path directory) throws InstantiationException,
        IllegalAccessException, IOException, SAXException, JAXBException {
        Manifest manifest = null;
        ValidatingXmlManager<Manifest> xmlManager = new ValidatingXmlManager<>(Manifest.class);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, entry -> {
            Path trueFile = DataFileManager.realSourceFile(entry);
            return Files.isRegularFile(trueFile)
                && entry.getFileName().toString().endsWith(FILENAME_SUFFIX);
        })) {
            for (Path entry : stream) {
                if (manifest != null) {
                    throw new IllegalStateException(
                        "Multiple manifest files identified in directory " + directory.toString());
                }
                manifest = xmlManager.unmarshal(DataFileManager.realSourceFile(entry).toFile());
                manifest.setName(entry.getFileName().toString());
            }
        }
        return manifest;
    }

    /**
     * Simple utility for manually generating a manifest from the contents of a directory. First
     * argument is the desired filename (including path, if not in the working directory). Second
     * argument is the dataset ID. Third argument is the path to go to for the files that get
     * manifested; if this is left out, the working directory will be used.
     */
    public static void main(String[] args) throws IOException, InstantiationException,
        IllegalAccessException, SAXException, JAXBException {
        String manifestName = args[0];
        long datasetId = Long.parseLong(args[1]);
        String manifestDir = System.getProperty("user.dir");
        if (args.length > 2) {
            manifestDir = args[2];
        }
        Manifest manifest = Manifest.generateManifest(Paths.get(manifestDir), datasetId);
        manifest.setName(manifestName);
        manifest.write(Paths.get(System.getProperty("user.dir")));
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(long datasetId) {
        this.datasetId = datasetId;
    }

    public ChecksumType getChecksumType() {
        return checksumType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (!validateName(name)) {
            throw new PipelineException("Name " + name + " invalid for manifest "
                + "(must end with \"" + FILENAME_SUFFIX + "\")");
        }
        this.name = name;
    }

    private boolean validateName(String nameForTest) {
        return nameForTest.endsWith(FILENAME_SUFFIX);
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public Date getImportTime() {
        return importTime;
    }

    public void setImportTime(Date importTime) {
        this.importTime = importTime;
    }

    public long getImportTaskId() {
        return importTaskId;
    }

    public void setImportTaskId(long importTaskId) {
        this.importTaskId = importTaskId;
    }

    public DataReceiptStatus getStatus() {
        return status;
    }

    public void setStatus(DataReceiptStatus status) {
        this.status = status;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public List<ManifestEntry> getManifestEntries() {
        return manifestEntries;
    }

    public void setManifestEntries(List<ManifestEntry> manifestEntries) {
        this.manifestEntries = manifestEntries;
    }

    /**
     * Models a single entry in a {@link Manifest}.
     *
     * @author PT
     */
    @XmlAccessorType(XmlAccessType.NONE)
    static class ManifestEntry {

        @XmlAttribute(required = true)
        private String name;

        @XmlAttribute(required = true)
        private long size;

        @XmlAttribute(required = true)
        private String checksum = "";

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public String getChecksum() {
            return checksum;
        }

        public void setChecksum(String checksum) {
            this.checksum = checksum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(checksum, name, size);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ManifestEntry other = (ManifestEntry) obj;
            return Objects.equals(checksum, other.checksum) && Objects.equals(name, other.name)
                && size == other.size;
        }
    }

    /**
     * Determines whether a {@link ManifestEntry} instance and an {@link AcknowledgementEntry}
     * instance refer to the same file. Specifically, this means that the name, size, and checksum
     * are the same in the two objects.
     */
    public static boolean manifestEntryAckEntryEquals(ManifestEntry mEntry,
        AcknowledgementEntry aEntry) {
        return mEntry.getName().equals(aEntry.getName()) && mEntry.getSize() == aEntry.getSize()
            && mEntry.getChecksum().equals(aEntry.getChecksum());
    }

}
