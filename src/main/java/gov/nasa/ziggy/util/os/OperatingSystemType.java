package gov.nasa.ziggy.util.os;

import static gov.nasa.ziggy.services.config.PropertyName.OPERATING_SYSTEM;
import static gov.nasa.ziggy.services.config.PropertyName.SUN_ARCH_DATA_MODEL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * This type is a container for operating system specific information and can be used for
 * portability across operating systems.
 *
 * @author Forrest Girouard
 */
public enum OperatingSystemType {
    DEFAULT("Linux", "LD_LIBRARY_PATH"),
    LINUX("Linux", "LD_LIBRARY_PATH"),
    MAC_OS_X("Darwin", "DYLD_LIBRARY_PATH");

    private static final Logger log = LoggerFactory.getLogger(OperatingSystemType.class);

    private final String name;
    private final String archDataModel;
    private final String sharedObjectPathEnvVar;

    OperatingSystemType(String name, String sharedObjectPathEnvVar) {
        this.name = name;
        archDataModel = ZiggyConfiguration.getInstance().getString(SUN_ARCH_DATA_MODEL.property());
        this.sharedObjectPathEnvVar = sharedObjectPathEnvVar;
    }

    public String getName() {
        return name;
    }

    public String getArchDataModel() {
        return archDataModel;
    }

    /**
     * @return e.g. "LD_LIBRARY_PATH"
     */
    public String getSharedObjectPathEnvVar() {
        return sharedObjectPathEnvVar;
    }

    public CpuInfo getCpuInfo() {
        return switch (this) {
            case DEFAULT -> new LinuxCpuInfo();
            case LINUX -> new LinuxCpuInfo();
            case MAC_OS_X -> new MacOSXCpuInfo();
        };
    }

    public MemInfo getMemInfo() {
        return switch (this) {
            case DEFAULT -> new LinuxMemInfo();
            case LINUX -> new LinuxMemInfo();
            case MAC_OS_X -> new MacOSXMemInfo();
        };
    }

    public ProcInfo getProcInfo(long pid) {
        return switch (this) {
            case DEFAULT -> new LinuxProcInfo(pid);
            case LINUX -> new LinuxProcInfo(pid);
            case MAC_OS_X -> new MacOSXProcInfo(pid);
        };
    }

    public ProcInfo getProcInfo() {
        return switch (this) {
            case DEFAULT -> new LinuxProcInfo();
            case LINUX -> new LinuxProcInfo();
            case MAC_OS_X -> new MacOSXProcInfo();
        };
    }

    public static final OperatingSystemType byName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null.");
        }

        for (OperatingSystemType type : OperatingSystemType.values()) {
            if (type != OperatingSystemType.DEFAULT
                && type.getName().equalsIgnoreCase(name.trim().replace(' ', '_'))) {
                return type;
            }
        }

        log.warn(name + ": unrecognized operating system, using default type");
        return OperatingSystemType.DEFAULT;
    }

    public static final OperatingSystemType byType(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null.");
        }

        for (OperatingSystemType type : OperatingSystemType.values()) {
            if (type.toString().equalsIgnoreCase(name.trim().replace(' ', '_'))) {
                return type;
            }
        }

        log.warn(name + ": unrecognized operating system, using default type");
        return OperatingSystemType.DEFAULT;
    }

    public static final OperatingSystemType getInstance() {
        return OperatingSystemType
            .byType(ZiggyConfiguration.getInstance().getString(OPERATING_SYSTEM.property()));
    }
}
