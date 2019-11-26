package com.journaldev.bean;

import com.domain.Address;
import com.domain.Customer;
import com.domain.Order;
import com.google.api.services.pubsub.Pubsub;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.sql.PreparedStatement;

public class PipeLineTest {

    /**
     * The Constant LOGGER.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(PipeLineTest.class);

    /**
     * The Constant INPUT_FILE_NAME.
     */
    private static final String INPUT_FILE_NAME = "gs://pubsub-json-demo/sample_json.txt";
    private static final String PROJECT_ID = "data-flow-runner-demo";
    private static final String GCP_API_KEY = "D:/IntellIJ workspace/pubsub-assignments/data-flow-runner-demo-580a315ec0b4.json";
    private static final String BUCKET_NAME = "pubsub-json-demo";
    private static final String FILE_NAME = "sample_json.json";
    /**
     * The Constant OUTPUT_TOPIC_NAME.
     */
    private static final String OUTPUT_TOPIC_NAME = "projects/data-flow-runner-demo/topics/myJSON_topic";
    static Logger logger = LoggerFactory.getLogger(PipeLineTest.class);

    /**
     * The main method.
     *
     * @param args the arguments
     */

    public static void main(String[] args) throws Exception {

        //Reading json file from bucket of GCP
        GoogleCredentials googleCredentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setCredentials(googleCredentials)
                .build()
                .getService();
        Blob blob = storage.get(BUCKET_NAME, FILE_NAME);
        //String json = new String(blob.getContent());

        ObjectMapper mapper = new ObjectMapper();
        Customer customerObject = new Customer(100, "abcd", 23, "M", false);
        Address address = new Address("Unit-2", "Belgrave St", "NSW", "Sydney", "Aus", 2027);
        Order order = new Order(101, customerObject, 12.0, "Not delivered", address, address, false);

        String json = mapper.writeValueAsString(order);
        json = json.trim().replaceAll("\n", "");
        //Persisting JSON file on pubsub topic
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        PCollection<String> output = pipeline.apply("Read JSON file", Create.<String>of(json));

        /*output.apply(ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(ProcessContext pc) {
                System.out.println("PCollection JSON : " + pc.element());
            }

        }));*/


        output.apply(PubsubIO.writeStrings().to(OUTPUT_TOPIC_NAME).withIdAttribute("UID-101"));


        final PCollection<String> output_json = pipeline.apply(PubsubIO.readStrings()
//                .fromTopic(OUTPUT_TOPIC_NAME));
                .fromSubscription("projects/" + PROJECT_ID + "/subscriptions/myJSON").withIdAttribute("UID-101"));

//        PCollection<PubsubMessage> messages = pipeline.apply(PubsubIO.readMessagesWithAttributes()
//                .fromSubscription("projects/" + PROJECT_ID + "/subscriptions/myJSON"));


        JdbcIO.Write<String> configuredWrite = JdbcIO.<String>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
                        .withUsername("postgres").withPassword("postgres"));

        output_json.apply(configuredWrite
                .withStatement("INSERT INTO address (line1, line2, state, district, country, pincode) VALUES (?,?,?,?,?,?)")
                //.withStatement("")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
                                                 private static final long serialVersionUID = 1L;

                                                 public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                                                     logger.info("DB Opertaion json data: " + element);
                                                     JSONObject jsonObject = new JSONObject(element);
                                                     JSONObject object_address = (JSONObject) jsonObject.get("shippingAddress");
                                                     preparedStatement.setString(1, object_address.get("line1").toString());
                                                     preparedStatement.setString(2, object_address.get("line2").toString());
                                                     preparedStatement.setString(3, object_address.get("state").toString());
                                                     preparedStatement.setString(4, object_address.get("district").toString());
                                                     preparedStatement.setString(5, object_address.get("country").toString());
                                                     preparedStatement.setString(6, object_address.get("pinCode").toString());
                                                 }
                                             }
                ));
        output_json.apply(configuredWrite.withStatement("INSERT INTO customer (id, name, age, gender) VALUES(?,?,?,?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
                                                 private static final long serialVersionUID = 1L;

                                                 public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                                                     logger.info("DB Opertaion json data: " + element);
                                                     JSONObject jsonObject = new JSONObject(element);
                                                     JSONObject object_customer = (JSONObject) jsonObject.get("customer");
                                                     preparedStatement.setInt(1, Integer.parseInt(object_customer.get("id").toString()));
                                                     preparedStatement.setString(2, object_customer.get("name").toString());
                                                     preparedStatement.setInt(3, Integer.parseInt(object_customer.get("age").toString()));
                                                     preparedStatement.setString(4, object_customer.get("gender").toString());
                                                 }
                                             }
                ));
        output_json.apply(configuredWrite.withStatement("INSERT INTO public.order (id, customerid, ordertotal, status, isgift) VALUES(?,?,?,?,?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
                                                 private static final long serialVersionUID = 1L;

                                                 public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                                                     logger.info("DB Opertaion json data: " + element);
                                                     //JSON file to Java object
                                                     //Staff staff = mapper.readValue(element, Staff.class);
                                                     JSONObject jsonObject = new JSONObject(element);
                                                     preparedStatement.setInt(1, Integer.parseInt(jsonObject.get("id").toString()));
                                                     preparedStatement.setInt(2, 101);
                                                     preparedStatement.setDouble(3, (Double) jsonObject.get("orderTotal"));
                                                     preparedStatement.setString(4, jsonObject.get("status").toString());
                                                     preparedStatement.setBoolean(5, (Boolean) jsonObject.get("gift"));
                                                 }
                                             }
                ));

        pipeline.run().waitUntilFinish(Duration.standardSeconds(60));
        LOGGER.debug("All done!!");
        System.exit(0);

    }
}

