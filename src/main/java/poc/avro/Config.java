package poc.avro;

import java.util.Properties;

public class Config {

    //Member definitions
    public Properties props;

    public static enum PropKeys {
        KAFKA_IP,
        KAFKA_SCHEMA_REGISTRY,
        KAKFA_TOPIC
    }

    public Config() {

        //Initialize Property Value container
        this.props = new Properties();
    }

    public void put(Object key, String value) {
        this.props.put(key, value);
    }

    public String get(Object key) {
        return (String) this.props.get(key);
    }

}
