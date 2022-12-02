package gov.nasa.ziggy.services.alert;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * @author Todd Klaus
 */
@Entity
@Table(name = "PI_ALERT")
public class AlertLog {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sg")
    @SequenceGenerator(name = "sg", initialValue = 1, sequenceName = "PI_PD_SEQ",
        allocationSize = 1)
    private long id;

    @Embedded
    private Alert alertData;

    /**
     * Default constructor for Hibernate use only.
     */
    AlertLog() {
    }

    public AlertLog(Alert alertData) {
        this.alertData = alertData;
    }

    /**
     * @return the id
     */
    public long getId() {
        return id;
    }

    public Alert getAlertData() {
        return alertData;
    }
}
