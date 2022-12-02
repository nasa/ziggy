package gov.nasa.ziggy.util.os;

public interface SysInfo {
    String get(String key);

    void put(String key, String value);
}
