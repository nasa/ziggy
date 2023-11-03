package gov.nasa.ziggy.buildutil;

/**
 * Provides a uniform system for ascertaining the combination of OS and CPU architecture of the
 * system.
 *
 * @author PT
 */
public enum SystemArchitecture {
    LINUX_INTEL, MAC_INTEL, MAC_M1;

    public static SystemArchitecture architecture() {

        String opSys = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch").toLowerCase();
        if (opSys.contains("linux")) {
            return LINUX_INTEL;
        }
        if (opSys.contains("mac")) {
            if (arch.contains("aarch")) {
                return MAC_M1;
            }
            return MAC_INTEL;
        }
        throw new IllegalStateException("System architecture unknown");
    }

    /**
     * Returns a new instance of {@link Selector}.
     *
     * @param <T> Class for Selector.
     * @param clazz Class for Selector.
     */
    public static <T> Selector<T> selector(Class<T> clazz) {
        return new Selector<>();
    }

    /**
     * Provides a compact notation for specifying a collection of objects, where one of the
     * collection is to be returned depending on the system architecture of the executing computer.
     * This can be used in place of switch statements (for systems that support them), or if-elseif
     * chains (for systems that do not support switch, such as Gradle scripts). Example:
     *
     * <pre>
     * String s = SystemArchitecture.selector(String.class)
     *     .linuxIntelObject("A")
     *     .macIntelObject("B")
     *     .macM1Object("C")
     *     .get();
     * </pre>
     *
     * will return "A" when executed on a Linux system, "B" when executed on a Mac using Intel
     * silicon, and "C" when executed on a Mac using Apple silicon.
     *
     * @author PT
     * @param <T> Class of object to be managed by the {@link Selector}.
     */
    public static class Selector<T> {
        private T linuxIntelObject;
        private T macM1Object;
        private T macIntelObject;

        private Selector() {
        }

        public Selector<T> linuxIntelObject(T object) {
            linuxIntelObject = object;
            return this;
        }

        public Selector<T> macIntelObject(T object) {
            macIntelObject = object;
            return this;
        }

        public Selector<T> macM1Object(T object) {
            macM1Object = object;
            return this;
        }

        SystemArchitecture architecture() {
            return SystemArchitecture.architecture();
        }

        public T get() {
            return switch (architecture()) {
                case LINUX_INTEL -> linuxIntelObject;
                case MAC_INTEL -> macIntelObject;
                case MAC_M1 -> macM1Object;
            };
        }
    }
}
