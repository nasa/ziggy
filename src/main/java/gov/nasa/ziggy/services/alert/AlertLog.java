package gov.nasa.ziggy.services.alert;

import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_AlertLog")
public class AlertLog {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_AlertLog_generator")
    @SequenceGenerator(name = "ziggy_AlertLog_generator", initialValue = 1,
        sequenceName = "ziggy_AlertLog_sequence", allocationSize = 1)
    private Long id;

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
    public Long getId() {
        return id;
    }

    public Alert getAlertData() {
        return alertData;
    }
}
