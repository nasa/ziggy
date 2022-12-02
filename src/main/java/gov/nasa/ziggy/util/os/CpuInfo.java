package gov.nasa.ziggy.util.os;

public interface CpuInfo extends SysInfo {
    String getNumCoresKey();

    int getNumCores();
}
