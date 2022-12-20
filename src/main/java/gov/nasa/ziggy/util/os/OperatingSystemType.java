package gov.nasa.ziggy.util.os;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.services.config.PropertyNames;

/**
 * This type is a container for operating system specific information and can be used for
 * portability across operating systems.
 *
 * @author Forrest Girouard
 */
public enum OperatingSystemType {
    DEFAULT("Linux", "LD_LIBRARY_PATH", LinuxMemInfo.class, LinuxCpuInfo.class,
        LinuxProcInfo.class),
    LINUX("Linux", "LD_LIBRARY_PATH", LinuxMemInfo.class, LinuxCpuInfo.class, LinuxProcInfo.class),
    MAC_OS_X("Darwin", "DYLD_LIBRARY_PATH", MacOSXMemInfo.class, MacOSXCpuInfo.class,
        MacOSXProcInfo.class);

    private static final Logger log = LoggerFactory.getLogger(OperatingSystemType.class);

    private final String name;
    private final String archDataModel;
    private final String sharedObjectPathEnvVar;
    private final Class<? extends MemInfo> memInfoClass;
    private final Class<? extends CpuInfo> cpuInfoClass;
    private final Class<? extends ProcInfo> procInfoClass;

    OperatingSystemType(String name, String sharedObjectPathEnvVar,
        Class<? extends MemInfo> memInfoClass, Class<? extends CpuInfo> cpuInfoClass,
        Class<? extends ProcInfo> procInfoClass) {
        this.name = name;
        archDataModel = System.getProperty(PropertyNames.ARCH_DATA_MODEL_PROPERTY_NAME);
        this.sharedObjectPathEnvVar = sharedObjectPathEnvVar;
        this.memInfoClass = memInfoClass;
        this.cpuInfoClass = cpuInfoClass;
        this.procInfoClass = procInfoClass;
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

    public CpuInfo getCpuInfo() throws Exception {
        return cpuInfoClass.getDeclaredConstructor().newInstance();
    }

    public MemInfo getMemInfo() throws Exception {
        return memInfoClass.getDeclaredConstructor().newInstance();
    }

    public ProcInfo getProcInfo(long pid) throws Exception {
        Class<?>[] procInfoArgs = new Class[] { long.class };
        return procInfoClass.getConstructor(procInfoArgs).newInstance(pid);
    }

    public ProcInfo getProcInfo() throws Exception {
        return procInfoClass.getDeclaredConstructor().newInstance();
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
            .byType(System.getProperty(PropertyNames.OPERATING_SYSTEM_PROPERTY_NAME));
    }
}
