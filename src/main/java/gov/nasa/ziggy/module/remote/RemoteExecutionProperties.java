package gov.nasa.ziggy.module.remote;

import gov.nasa.ziggy.services.config.ZiggyConfiguration;

public class RemoteExecutionProperties {

    public static final String HOST_PROPERTY = "remote.host";
    public static final String USER_PROPERTY = "remote.user";
    public static final String GROUP_PROPERTY = "remote.group";

    /**
     * The hostnames that can be used on Pleiades.
     *
     * @return Names of hosts, in order from most- to least-desired to use.
     */
    public static String[] getHost() {
        String hosts = ZiggyConfiguration.getInstance().getString(HOST_PROPERTY, "");
        if (hosts.isEmpty()) {
            return new String[0];
        } else {
            return hosts.split(";");
        }
    }

    /**
     * The username to use on Pleiades.
     *
     * @return The username to be used.
     */
    public static String getUser() {
        return ZiggyConfiguration.getInstance().getString(USER_PROPERTY, "");
    }

    /**
     * The user group to use on Pleiades.
     *
     * @return The group to be used.
     */
    public static String getGroup() {
        return ZiggyConfiguration.getInstance().getString(GROUP_PROPERTY, "");
    }

}
