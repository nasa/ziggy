package gov.nasa.ziggy.services.process;

import java.io.Serializable;

import gov.nasa.ziggy.util.HostNameUtils;

/**
 * Holds basic info about a pipeline process. Used for logging and UI
 *
 * @author Todd Klaus
 */
public class ProcessInfo implements Serializable {
    private static final long serialVersionUID = -2089578114844840384L;

    private String name;
    private String host;
    private int pid;
    private int jvmid;

    private final String key;

    public ProcessInfo(String name, String host, int pid, int jvmid) {
        this.name = name;
        this.host = HostNameUtils.callerHostNameOrLocalhost(host);
        this.pid = pid;
        this.jvmid = jvmid;

        key = name + ":" + host + ":" + pid + "(" + jvmid + ")";
    }

    public ProcessInfo(ProcessInfo other) {
        name = other.name;
        host = HostNameUtils.callerHostNameOrLocalhost(other.host);
        pid = other.pid;
        jvmid = other.jvmid;

        key = name + ":" + host + ":" + pid + "(" + jvmid + ")";
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return the jvmid
     */
    public int getJvmid() {
        return jvmid;
    }

    /**
     * @param jvmid the jvmid to set
     */
    public void setJvmid(int jvmid) {
        this.jvmid = jvmid;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the pid
     */
    public int getPid() {
        return pid;
    }

    /**
     * @param pid the pid to set
     */
    public void setPid(int pid) {
        this.pid = pid;
    }

}
