package poc.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import poc.avro.Config;

public class AvroProducer {

    public void writeToTopic(Config envProps) {

        //Set properties for the producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.get(Config.PropKeys.KAFKA_IP));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.get(Config.PropKeys.KAFKA_SCHEMA_REGISTRY));

        Producer<String, Object> producer = new KafkaProducer<>(producerProps);

        //Create a new Avro schema using a hardcoded schema definition
        String userSchemaDefinition = "{\"type\": \"record\"," +
                " \"name\": \"myrecord\"," +
                " \"fields\": [" +
                "     {\"name\": \"id\", \"type\": \"int\"}," +
                "     {\"name\": \"fname\",  \"type\": \"string\"}," +
                "     {\"name\": \"lname\",  \"type\": \"string\"}," +
                "     {\"name\": \"phone_number\",  \"type\": \"string\"}," +
                "     {\"name\": \"age\",  \"type\": \"int\"}," +
                "     {\"name\": \"popularityIndex\",  \"type\": \"int\"}," +
                "     {\"name\": \"birthplace\",  \"type\": \"string\"}," +
                "     {" +
                "      \"name\": \"emailAddresses\"," +
                "      \"type\": {" +
                "          \"type\": \"array\"," +
                "          \"items\": {" +
                "          \"name\": \"EmailAddress\"," +
                "          \"type\": \"record\"," +
                "          \"fields\": [" +
                "            {" +
                "              \"name\": \"email\"," +
                "              \"type\": \"string\"" +
                "            }," +
                "            {" +
                "              \"name\": \"primary\"," +
                "              \"type\": \"boolean\"" +
                "            }" +
                "          ]" +
                "        }" +
                "        }" +
                "    }" +
                " ]" +
                "}";

        Schema.Parser schemaParser = new Schema.Parser();
        Schema avroSchema = schemaParser.parse(userSchemaDefinition);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);

        avroRecord.put("id",12345);
        avroRecord.put("fname", "Harpo");
        avroRecord.put("lname", "Baldwin");
        avroRecord.put("phone_number", "2045762435");
        avroRecord.put("age",61);
        avroRecord.put("birthplace","Memphis");
        avroRecord.put("popularityIndex", 100);

        Schema childSchema = avroRecord.getSchema().getField("emailAddresses").schema().getElementType();

        List<GenericRecord> addressList = new ArrayList<>();

        for(int i=0; i < 3; i++) {

            String emailString = "";

            switch (i) {
                case 0:
                    emailString = "harpo.baldwin@gmail.com";
                    break;

                case 1:
                    emailString = "harpo.baldwin@outlook.com";
                    break;

                case 2:
                    emailString = "baldwinbros@hotmail.com";
                    break;
            }

            GenericRecord emailAddress = new GenericData.Record(childSchema);

            emailAddress.put("email", emailString);

            if (i == 0) {
                emailAddress.put("primary", true);
            } else {
                emailAddress.put("primary", false);
            }


            addressList.add(emailAddress);

            avroRecord.put("emailAddresses", addressList);

        }

        System.out.println(avroRecord);

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(envProps.get(Config.PropKeys.KAKFA_TOPIC),null,avroRecord);
        producer.send(producerRecord);
        producer.flush();
        producer.close();

    }
}
