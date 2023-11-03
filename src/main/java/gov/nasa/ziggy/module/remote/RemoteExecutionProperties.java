package gov.nasa.ziggy.module.remote;

import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_GROUP;
import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_HOST;
import static gov.nasa.ziggy.services.config.PropertyName.REMOTE_USER;

import gov.nasa.ziggy.services.config.ZiggyConfiguration;

public class RemoteExecutionProperties {

    /**
     * The hostnames that can be used on Pleiades.
     *
     * @return names of hosts, in order from most- to least-desired to use
     */
    public static String[] getHost() {
        String hosts = ZiggyConfiguration.getInstance().getString(REMOTE_HOST.property(), "");
        if (hosts.isEmpty()) {
            return new String[0];
        }
        return hosts.split(";");
    }

    /**
     * The username to use on Pleiades.
     *
     * @return the username to be used
     */
    public static String getUser() {
        return ZiggyConfiguration.getInstance().getString(REMOTE_USER.property(), "");
    }

    /**
     * The user group to use on Pleiades.
     *
     * @return the group to be used
     */
    public static String getGroup() {
        return ZiggyConfiguration.getInstance().getString(REMOTE_GROUP.property(), "");
    }
}
