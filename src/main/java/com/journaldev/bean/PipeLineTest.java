package com.journaldev.bean;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        String json = new String(blob.getContent());

        //Persisting JSON file on pubsub topic
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        PCollection<String> output = pipeline.apply("Read JSON file", Create.of(new String(json)));
       /* PCollection<String> output = input.apply("Read the file", ParDo.of(new DoFn<String, String>() {
            private static final long serialVersionUID = 1L;

            *//**
             * Process element.
             *
             * @param context
             *            the context
             *//*
            @ProcessElement
            public void processElement(ProcessContext context) {
                LOGGER.debug("#Processing record : {}", context.element());
                context.output(context.element());
            }
        }));*/
        output.apply(PubsubIO.writeStrings().to(OUTPUT_TOPIC_NAME));

        //Persisting that json file from pubsub topic to PostgresSQL
        final PCollection<String> output_json = pipeline.apply(PubsubIO.readStrings().fromTopic(OUTPUT_TOPIC_NAME));
        output_json.apply(JdbcIO.<String>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
                        .withUsername("postgres").withPassword("postgres"))
                .withStatement("INSERT INTO employee (emp_id, emp_name) VALUES (?,?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
                 private static final long serialVersionUID = 1L;
                 public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                                                     logger.info("DB Opertaion json data: " + element);
                     ObjectMapper mapper = new ObjectMapper();

                     //JSON file to Java object
                     //Staff staff = mapper.readValue(element, Staff.class);

                    JSONObject jsonObject = new JSONObject(element);
                    //String target = jsonObject.getString("target");
                    JSONArray columnValue = jsonObject.getJSONArray("values");
                    System.out.println(element);
                    preparedStatement.setInt(1, Integer.parseInt(columnValue.get(0).toString()));
                    preparedStatement.setString(2, columnValue.get(1).toString());
                                                 }
                                             }
                ));
        pipeline.run();
        LOGGER.debug("All done!!");
    }
}

