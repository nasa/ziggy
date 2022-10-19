package gov.nasa.ziggy.data.management;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

import gov.nasa.ziggy.data.management.Manifest.ManifestEntry;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.xml.HasXmlSchemaFilename;
import gov.nasa.ziggy.pipeline.xml.ValidatingXmlManager;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.alert.AlertService.Severity;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * Models an acknowledgement of a data receipt {@link Manifest}. An acknowledgement contains an
 * entry for every file in the manifest that shows whether that file was successfully transferred
 * without corruption, plus has an overall status value that is good if all files transferred
 * correctly, false otherwise.
 *
 * @see Manifest
 * @author PT
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class Acknowledgement implements HasXmlSchemaFilename {

    private static final String SCHEMA_FILENAME = "manifest-ack.xsd";
    static final String FILENAME_SUFFIX = "-ack";

    private static final double DEFAULT_MAX_FAILURE_PERCENTAGE = 100;

    // Thread pool for checksum calculations
    static ExecutorService checksumThreadPool = Executors
        .newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    // Add a shutdown hook for the checksum thread pool
    static {
        ZiggyShutdownHook.addShutdownHook(() -> checksumThreadPool.shutdownNow());
    }

    @XmlAttribute(required = true)
    private long datasetId;

    @XmlAttribute(required = true)
    private final ChecksumType checksumType;

    private String name = "";

    @XmlAttribute(required = true)
    private int fileCount;

    @XmlAttribute(required = true)
    @XmlJavaTypeAdapter(DataReceiptStatus.DataReceiptStatusAdapter.class)
    private DataReceiptStatus transferStatus;

    @XmlElement(required = true, name = "file")
    private List<AcknowledgementEntry> acknowledgementEntries = new ArrayList<>();

    @Override
    public String getXmlSchemaFilename() {
        return SCHEMA_FILENAME;
    }

    public Acknowledgement() {
        checksumType = Manifest.CHECKSUM_TYPE;
    }

    public Acknowledgement(ChecksumType checksumType) {
        this.checksumType = checksumType;
    }

    /**
	 * Writes the acknowledgement to the specified directory. The manifest name must
	 * be set to a valid value; if not, a {@link PipelineException} will occur.
	 *
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 */
    public void write(Path directory)
			throws InstantiationException, IllegalAccessException, SAXException, JAXBException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {

        ValidatingXmlManager<Acknowledgement> xmlManager = new ValidatingXmlManager<>(
            Acknowledgement.class);
        directory.toFile().mkdirs();

        Path fullPath = directory.resolve(name);
        xmlManager.marshal(this, fullPath.toFile());
    }

    /**
     * Generates the filename for an acknowledgement using the filename of the corresponding
     * {@link Manifest}. The portion of the filename prior to the file type extension is located and
     * a suffix of "-ack" is added, thus a manifest name of "something-manifest.xml" becomes an
     * acknowledgement name "something-manifest-ack.xml".
     */
    public static String nameFromManifestName(Manifest manifest) {

        String manifestName = manifest.getName();
        if (manifestName.isEmpty()) {
            throw new PipelineException(
                "Unable to generate Acknowledgement name from unnamed Manifest");
        }
        String manifestFileType = FilenameUtils.getExtension(manifestName);
        int manifestTypeLength = manifestFileType.length() + 1;
        String baseName = manifestName.substring(0, manifestName.length() - manifestTypeLength);
        return baseName + FILENAME_SUFFIX + "." + manifestFileType;
    }

    /**
     * Determines whether a valid {@link Acknowledgement} exists for a specified {@link Manifest} in
     * a specified working directory. This means: an {@link Acknowledgement} that matches the name
     * expected given the name of the {@link Manifest}, and one that has a value of
     * {@link DataReceiptStatus#VALID} for its transfer status. The purpose of this is to infer
     * whether the manifest has already been acknowledged, which can occur if the data receipt
     * process is interrupted after validation but prior to completion of file imports.
     */
    public static boolean validAcknowledgementExists(Path workingDir, Manifest manifest) {
        Path acknowledgementPath = workingDir.resolve(nameFromManifestName(manifest));
        if (!Files.exists(acknowledgementPath)) {
            return false;
        }
        ValidatingXmlManager<Acknowledgement> xmlManager;
        try {
            xmlManager = new ValidatingXmlManager<>(Acknowledgement.class);
            Acknowledgement ack = xmlManager.unmarshal(acknowledgementPath.toFile());
            return ack.transferStatus.equals(DataReceiptStatus.VALID);
		} catch (InstantiationException | IllegalAccessException | SAXException | JAXBException
				| IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new PipelineException(
                "Unable to read acknowledgement " + acknowledgementPath.toString(), e);
        }
    }

    public Map<String, AcknowledgementEntry> fileNameToAckEntry() {
        Map<String, AcknowledgementEntry> fileNameToAckEntry = new HashMap<>();
        for (AcknowledgementEntry entry : acknowledgementEntries) {
            fileNameToAckEntry.put(entry.getName(), entry);
        }
        return fileNameToAckEntry;
    }

    public List<String> namesOfValidFiles() {
        return acknowledgementEntries.stream()
            .filter(s -> s.getTransferStatus().equals(DataReceiptStatus.PRESENT))
            .filter(s -> s.getValidationStatus().equals(DataReceiptStatus.VALID))
            .map(AcknowledgementEntry::getName)
            .collect(Collectors.toList());
    }

    /**
     * Generates an {@link Acknowledgement} instance from a {@link Manifest}. This entails
     * performing all the necessary validation checks between the contents of the Manifest and the
     * files it references. The status field of the {@link Manifest} is also set to match the
     * corresponding field in the acknowledgement.
     *
     * @param manifest {@link Manifest} file to be acknowledged.
     * @param dir Location of the files referenced in the manifest.
     * @param taskId ID of the {@link PipelineTask} that is performing the manifest validation.
     * @return {@link Acknowledgement} that includes validation status of all files referenced in
     * the manifest.
     */
    public static Acknowledgement of(Manifest manifest, Path dir, long taskId) {

        Acknowledgement acknowledgement = new Acknowledgement(manifest.getChecksumType());
        acknowledgement.setDatasetId(manifest.getDatasetId());
        acknowledgement.setName(nameFromManifestName(manifest));
        acknowledgement.setFileCount(manifest.getFileCount());

        int maxValidationFailures = (int) (acknowledgement.getFileCount()
            * ZiggyConfiguration.getInstance()
                .getDouble(PropertyNames.MAX_FAILURE_PERCENTAGE_PROP_NAME,
                    DEFAULT_MAX_FAILURE_PERCENTAGE)
            / 100);

        DataReceiptStatus manifestStatus = DataReceiptStatus.VALID;

        // perform entry-by-entry validation in the dedicated thread pool for same.
        List<Future<AcknowledgementEntry>> futures = new ArrayList<>();
        for (ManifestEntry manifestEntry : manifest.getManifestEntries()) {
            futures.add(checksumThreadPool.submit(() -> {
                AcknowledgementEntry ackEntry = null;
                try {
                    ackEntry = AcknowledgementEntry.of(manifestEntry, dir,
                        acknowledgement.getChecksumType());
                } catch (IOException e) {
                    throw new PipelineException("Unable to calculate checksum or file size "
                        + " of file " + manifestEntry.getName(), e);
                }
                return ackEntry;
            }));
        }

        // Capture the AcknowledgementEntry instances as they complete.
        int validationFailures = 0;
        for (Future<?> future : futures) {
            try {
                AcknowledgementEntry ackEntry = (AcknowledgementEntry) future.get();
                acknowledgement.getAcknowledgementEntries().add(ackEntry);
                manifestStatus = manifestStatus.and(ackEntry.overallStatus());
                if (ackEntry.overallStatus().equals(DataReceiptStatus.INVALID)) {

                    // Issue a warning as soon as we know that there are validation
                    // failures.
                    if (validationFailures == 0) {
                        AlertService.getInstance()
                            .generateAndBroadcastAlert("Data Receipt", taskId, Severity.WARNING,
                                "File validation errors encountered");
                    }
                    validationFailures++;

                    // If there are too many validation failures, terminate early and
                    // write an incomplete acknowledgement so the user can at least see
                    // which files failed prior to termination.
                    if (validationFailures >= maxValidationFailures) {
                        AlertService.getInstance()
                            .generateAndBroadcastAlert("Data Receipt", taskId, Severity.ERROR,
                                "Exceeded " + maxValidationFailures + ", terminating");
                        break;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new PipelineException("Exception occurred waiting for checksum results", e);
            }
        }

        manifest.setStatus(manifestStatus);
        acknowledgement.setTransferStatus(manifestStatus);

        return acknowledgement;
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
        this.name = name;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public DataReceiptStatus getTransferStatus() {
        return transferStatus;
    }

    public void setTransferStatus(DataReceiptStatus transferStatus) {
        this.transferStatus = transferStatus;
    }

    public List<AcknowledgementEntry> getAcknowledgementEntries() {
        return acknowledgementEntries;
    }

    public void setAcknowledgementFiles(List<AcknowledgementEntry> acknowledgementEntries) {
        this.acknowledgementEntries = acknowledgementEntries;
    }

    /**
     * Models a single entry in an {@link Acknowledgement}. Each entry has the name of the file it
     * refers to as well as the size and checksum of the file calculated when that file was
     * validated against its corresponding {@link ManifestEntry} (i.e., the size and checksum
     * calculated by the recipient, as opposed to the values in the {@link ManifestEntry}, which are
     * calculated by the sender). The AcknowledgementEntry also contains the transfer status of the
     * file (i.e., was the recipient able to find it at all) and the validation status of the file
     * (i.e., did the size and checksum calculated by the recipient match the values calculated by
     * the sender).
     *
     * @author PT
     */
    @XmlAccessorType(XmlAccessType.NONE)
    static class AcknowledgementEntry {

        @XmlAttribute(required = true)
        private String name;

        @XmlAttribute(required = true)
        private long size;

        @XmlAttribute(required = true)
        private String checksum = "";

        @XmlAttribute(required = true)
        @XmlJavaTypeAdapter(DataReceiptStatus.DataReceiptStatusAdapter.class)
        private DataReceiptStatus transferStatus;

        @XmlAttribute(required = true)
        @XmlJavaTypeAdapter(DataReceiptStatus.DataReceiptStatusAdapter.class)
        private DataReceiptStatus validationStatus;

        public DataReceiptStatus overallStatus() {
            return transferStatus.and(validationStatus);
        }

        /**
         * Generates the {@link AcknowledgementEntry} that corresponds to a given
         * {@link ManifestEntry}. Validation of the file represented by the ManifestEntry is
         * performed, and the validation results are stored in the AcknowledgementEntry.
         */
        public static AcknowledgementEntry of(ManifestEntry manifestEntry, Path dir,
            ChecksumType checksumType) throws IOException {

            AcknowledgementEntry ackEntry = new AcknowledgementEntry();
            ackEntry.setName(manifestEntry.getName());
            Path file = dir.resolve(manifestEntry.getName());
            Path realFile = null;
            ackEntry.setTransferStatus(DataReceiptStatus.ABSENT);
            ackEntry.setValidationStatus(DataReceiptStatus.INVALID);

            // Start by making sure the file exists and is a regular file (or symlink to same)
            if (Files.exists(file) || Files.isSymbolicLink(file)) {
                realFile = DataFileManager.realSourceFile(file);
                ackEntry.setTransferStatus(DataReceiptStatus.PRESENT);
            }
            if (ackEntry.getTransferStatus() == DataReceiptStatus.ABSENT) {
                return ackEntry;
            }

            // If the transfer was successful, we can set the validation status
            ackEntry.setSize(Files.size(realFile));
            ackEntry.setChecksum(checksumType.checksum(realFile));
            if (ackEntry.getSize() == manifestEntry.getSize()
                && ackEntry.getChecksum().equals(manifestEntry.getChecksum())) {
                ackEntry.setValidationStatus(DataReceiptStatus.VALID);
            }
            return ackEntry;

        }

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

        public DataReceiptStatus getTransferStatus() {
            return transferStatus;
        }

        public void setTransferStatus(DataReceiptStatus transferStatus) {
            this.transferStatus = transferStatus;
        }

        public DataReceiptStatus getValidationStatus() {
            return validationStatus;
        }

        public void setValidationStatus(DataReceiptStatus validationStatus) {
            this.validationStatus = validationStatus;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, checksum, size, transferStatus, validationStatus);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
				return true;
			}
            if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
            AcknowledgementEntry other = (AcknowledgementEntry) obj;
            return Objects.equals(name, other.name) && Objects.equals(checksum, other.checksum)
                && size == other.size && transferStatus == other.transferStatus
                && validationStatus == other.validationStatus;
        }

    }
}
