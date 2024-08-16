package gov.nasa.ziggy.data.datastore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.ZiggyDirectoryRule;
import gov.nasa.ziggy.ZiggyPropertyRule;
import gov.nasa.ziggy.services.config.DirectoryProperties;

/**
 * Static methods that can be used to prepare datastore-related data objects for unit tests.
 *
 * @author PT
 */
public class DatastoreTestUtils {

    /**
     * Returns datastore nodes based on a partial implementation of the TESS DR and CAL locations.
     */
    public static Map<String, DatastoreNode> datastoreNodesByFullPath() {
        Map<String, DatastoreNode> datastoreNodesByFullPath = new HashMap<>();

        DatastoreNode sectorNode = new DatastoreNode("sector", true);
        setFullPath(sectorNode, null);
        datastoreNodesByFullPath.put(sectorNode.getFullPath(), sectorNode);

        DatastoreNode mdaNode = new DatastoreNode("mda", false);
        setFullPath(mdaNode, sectorNode);
        sectorNode.setChildNodeFullPaths(List.of("sector/mda"));
        datastoreNodesByFullPath.put(mdaNode.getFullPath(), mdaNode);

        DatastoreNode drNode = new DatastoreNode("dr", false);
        setFullPath(drNode, mdaNode);
        datastoreNodesByFullPath.put(drNode.getFullPath(), drNode);

        DatastoreNode calNode = new DatastoreNode("cal", false);
        setFullPath(calNode, mdaNode);
        mdaNode.setChildNodeFullPaths(List.of("sector/mda/dr", "sector/mda/cal"));
        datastoreNodesByFullPath.put(calNode.getFullPath(), calNode);

        DatastoreNode drPixelNode = new DatastoreNode("pixels", false);
        setFullPath(drPixelNode, drNode);
        drNode.setChildNodeFullPaths(List.of("sector/mda/dr/pixels"));
        datastoreNodesByFullPath.put(drPixelNode.getFullPath(), drPixelNode);

        DatastoreNode drCadenceTypeNode = new DatastoreNode("cadenceType", true);
        setFullPath(drCadenceTypeNode, drPixelNode);
        drPixelNode.setChildNodeFullPaths(List.of("sector/mda/dr/pixels/cadenceType"));
        datastoreNodesByFullPath.put(drCadenceTypeNode.getFullPath(), drCadenceTypeNode);

        DatastoreNode drPixelTypeNode = new DatastoreNode("pixelType", true);
        setFullPath(drPixelTypeNode, drCadenceTypeNode);
        drCadenceTypeNode
            .setChildNodeFullPaths(List.of("sector/mda/dr/pixels/cadenceType/pixelType"));
        datastoreNodesByFullPath.put(drPixelTypeNode.getFullPath(), drPixelTypeNode);

        DatastoreNode drChannelNode = new DatastoreNode("channel", true);
        setFullPath(drChannelNode, drPixelTypeNode);
        drPixelTypeNode
            .setChildNodeFullPaths(List.of("sector/mda/dr/pixels/cadenceType/pixelType/channel"));
        datastoreNodesByFullPath.put(drChannelNode.getFullPath(), drChannelNode);

        DatastoreNode calPixelNode = new DatastoreNode("pixels", false);
        setFullPath(calPixelNode, calNode);
        calNode.setChildNodeFullPaths(List.of("sector/mda/cal/pixels"));
        datastoreNodesByFullPath.put(calPixelNode.getFullPath(), calPixelNode);

        DatastoreNode calCadenceTypeNode = new DatastoreNode("cadenceType", true);
        setFullPath(calCadenceTypeNode, calPixelNode);
        calPixelNode.setChildNodeFullPaths(List.of("sector/mda/cal/pixels/cadenceType"));
        datastoreNodesByFullPath.put(calCadenceTypeNode.getFullPath(), calCadenceTypeNode);

        DatastoreNode calPixelTypeNode = new DatastoreNode("pixelType", true);
        setFullPath(calPixelTypeNode, calCadenceTypeNode);
        calCadenceTypeNode
            .setChildNodeFullPaths(List.of("sector/mda/cal/pixels/cadenceType/pixelType"));
        datastoreNodesByFullPath.put(calPixelTypeNode.getFullPath(), calPixelTypeNode);

        DatastoreNode calChannelNode = new DatastoreNode("channel", true);
        setFullPath(calChannelNode, calPixelTypeNode);
        calPixelTypeNode
            .setChildNodeFullPaths(List.of("sector/mda/cal/pixels/cadenceType/pixelType/channel"));
        datastoreNodesByFullPath.put(calChannelNode.getFullPath(), calChannelNode);

        return datastoreNodesByFullPath;
    }

    private static void setFullPath(DatastoreNode node, DatastoreNode parent) {
        String parentPath = parent != null ? parent.getFullPath() : null;
        node.setFullPath(DatastoreWalker.fullPathFromParentPath(node.getName(), parentPath));
    }

    /** Returns regexps based on a partial implementation of DR and CAL. */
    public static Map<String, DatastoreRegexp> regexpsByName() {
        Map<String, DatastoreRegexp> regexpsByName = new HashMap<>();

        DatastoreRegexp sectorRegexp = new DatastoreRegexp("sector", "(sector-[0-9]{4})");
        regexpsByName.put(sectorRegexp.getName(), sectorRegexp);

        DatastoreRegexp cadenceTypeRegexp = new DatastoreRegexp("cadenceType",
            "(target|ffi|fast-target)");
        regexpsByName.put(cadenceTypeRegexp.getName(), cadenceTypeRegexp);

        DatastoreRegexp pixelTypeRegexp = new DatastoreRegexp("pixelType", "(science|collateral)");
        regexpsByName.put(pixelTypeRegexp.getName(), pixelTypeRegexp);

        DatastoreRegexp channelRegexp = new DatastoreRegexp("channel", "([1-4]:[1-4]:[A-D])");
        regexpsByName.put(channelRegexp.getName(), channelRegexp);

        return regexpsByName;
    }

    /** Returns data file types based on CAL inputs and outputs. */
    public static Map<String, DataFileType> dataFileTypesByName() {

        Map<String, DataFileType> dataFileTypesByName = new HashMap<>();

        DataFileType uncalibratedSciencePixelType = new DataFileType(
            "uncalibrated science pixel values",
            "sector/mda/dr/pixels/cadenceType/pixelType$science/channel",
            "(uncalibrated-pixels-[0-9]+)\\.science\\.nc");
        dataFileTypesByName.put(uncalibratedSciencePixelType.getName(),
            uncalibratedSciencePixelType);

        DataFileType uncalibratedCollateralPixelType = new DataFileType(
            "uncalibrated collateral pixel values",
            "sector/mda/dr/pixels/cadenceType/pixelType$collateral/channel",
            "(uncalibrated-pixels-[0-9]+)\\.collateral\\.nc");
        dataFileTypesByName.put(uncalibratedCollateralPixelType.getName(),
            uncalibratedCollateralPixelType);

        DataFileType calibratedSciencePixelType = new DataFileType(
            "calibrated science pixel values",
            "sector/mda/cal/pixels/cadenceType/pixelType$science/channel",
            "(everyone-needs-me-[0-9]+)\\.nc");
        dataFileTypesByName.put(calibratedSciencePixelType.getName(), calibratedSciencePixelType);

        DataFileType calibratedCollateralPixelType = new DataFileType(
            "calibrated collateral pixel values",
            "sector/mda/cal/pixels/cadenceType/pixelType$collateral/channel",
            "(outputs-file-[0-9]+)\\.nc");
        dataFileTypesByName.put(calibratedCollateralPixelType.getName(),
            calibratedCollateralPixelType);

        return dataFileTypesByName;
    }

    public static Map<String, DataFileType> dataFileTypesByNameRegexpsInFileName() {

        Map<String, DataFileType> dataFileTypesByName = new HashMap<>();

        DataFileType uncalibratedSciencePixelType = new DataFileType(
            "uncalibrated science pixel values", "sector/mda/dr/pixels/cadenceType",
            "pixelType$science/channel/(uncalibrated-pixels-[0-9]+)\\.science\\.nc");
        dataFileTypesByName.put(uncalibratedSciencePixelType.getName(),
            uncalibratedSciencePixelType);

        DataFileType uncalibratedCollateralPixelType = new DataFileType(
            "uncalibrated collateral pixel values", "sector/mda/dr/pixels/cadenceType",
            "pixelType$collateral/channel/(uncalibrated-pixels-[0-9]+)\\.collateral\\.nc");
        dataFileTypesByName.put(uncalibratedCollateralPixelType.getName(),
            uncalibratedCollateralPixelType);

        DataFileType calibratedSciencePixelType = new DataFileType(
            "calibrated science pixel values", "sector/mda/cal/pixels/cadenceType",
            "pixelType$science/channel/(everyone-needs-me-[0-9]+)\\.nc");
        dataFileTypesByName.put(calibratedSciencePixelType.getName(), calibratedSciencePixelType);

        DataFileType calibratedCollateralPixelType = new DataFileType(
            "calibrated collateral pixel values", "sector/mda/cal/pixels/cadenceType",
            "pixelType$collateral/channel/(outputs-file-[0-9]+)\\.nc");
        dataFileTypesByName.put(calibratedCollateralPixelType.getName(),
            calibratedCollateralPixelType);

        return dataFileTypesByName;
    }

    /**
     * Creates a subset of datastore directories for CAL inputs and outputs. The resulting
     * directories are created in the directory indicated by the DATASTORE_ROOT_DIR. To use this
     * method, do the following in the caller:
     * <ol>
     * <li>Use {@link ZiggyDirectoryRule} to create a directory for test artifacts.
     * <li>Use {@link ZiggyPropertyRule} to set the DATASTORE_ROOT_DIR to a subdirectory in the test
     * artifact directory.
     */
    public static void createDatastoreDirectories() throws IOException {
        Path datastoreRoot = DirectoryProperties.datastoreRootDir();

        // Start with sector 2 uncalibrated target pixels for 1:1:A and 1:1:B.
        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:B"));

        // Sector 2 uncalibrated FFI pixels for 1:1:A and 1:1:B.
        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:B"));

        // Sector 3 uncalibrated target pixels for 1:1:A and 1:1:B.
        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:B"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:B"));

        // Sector 3 uncalibrated FFI pixels for 1:1:A and 1:1:B.
        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("dr")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:B"));

        // Sector 2 calibrated target pixels for 1:1:A.
        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("science")
            .resolve("1:1:A"));

        Files.createDirectories(datastoreRoot.resolve("sector-0002")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("target")
            .resolve("collateral")
            .resolve("1:1:A"));

        // Sector 3 calibrated FFI pixels for 1:1:B.
        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("science")
            .resolve("1:1:B"));

        Files.createDirectories(datastoreRoot.resolve("sector-0003")
            .resolve("mda")
            .resolve("cal")
            .resolve("pixels")
            .resolve("ffi")
            .resolve("collateral")
            .resolve("1:1:B"));
    }
}
