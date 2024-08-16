package gov.nasa.ziggy.services.database;

import java.util.Properties;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;

/**
 * Holds basic JDBC connect info.
 * <p>
 * By default, set using the hibernate properties from the config service.
 *
 * @author Todd Klaus
 */
public class ConnectInfo {
    private String driverName;
    private String url;
    private String username;
    private String password;

    public ConnectInfo() {
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();

        driverName = ZiggyHibernateConfiguration.driverClassName();
        url = config.getString(PropertyName.HIBERNATE_URL.property());
        username = config.getString(PropertyName.HIBERNATE_USERNAME.property());
        password = config.getString(PropertyName.HIBERNATE_PASSWORD.property());
    }

    public ConnectInfo(Properties databaseProperties) {
        driverName = ZiggyHibernateConfiguration.driverClassName();
        url = databaseProperties.getProperty(PropertyName.HIBERNATE_URL.property());
        username = databaseProperties.getProperty(PropertyName.HIBERNATE_USERNAME.property());
        password = databaseProperties.getProperty(PropertyName.HIBERNATE_PASSWORD.property());
    }

    public ConnectInfo(String driverName, String url, String username, String password) {
        this.driverName = driverName;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ConnectInfo [driverName=")
            .append(driverName)
            .append(", url=")
            .append(url)
            .append(", username=")
            .append(username)
            .append(", password=")
            .append(StringUtils.isBlank(password) ? "not set" : "set")
            .append("]");
        return builder.toString();
    }
}
