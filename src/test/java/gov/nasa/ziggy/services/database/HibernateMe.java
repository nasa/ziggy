package gov.nasa.ziggy.services.database;

public class HibernateMe {
    private int id;
    private String value;

    public HibernateMe() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
