package com.journaldev.bean;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
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
    private static final String FILE_NAME = "sample_json.txt";

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

        GoogleCredentials googleCredentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setCredentials(googleCredentials)
                .build()
                .getService();
        Blob blob = storage.get(BUCKET_NAME, FILE_NAME);
        String json = new String(blob.getContent());

        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        PCollection<String> input = pipeline.apply("Read JSON from text file", Create.of(new String(json)));
        PCollection<String> output = input.apply("Read the file", ParDo.of(new DoFn<String, String>() {
            private static final long serialVersionUID = 1L;

            /**
             * Process element.
             *
             * @param context
             *            the context
             */
            @ProcessElement
            public void processElement(ProcessContext context) {
                LOGGER.debug("#Processing record : {}", context.element());
                context.output(context.element());
            }
        }));

        output.apply(PubsubIO.writeStrings().to(OUTPUT_TOPIC_NAME));

        PCollection<String> output_json = pipeline.apply(PubsubIO.readStrings().fromTopic(OUTPUT_TOPIC_NAME));
        output_json.apply(JdbcIO.<String>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
                        .withUsername("postgres").withPassword("postgres"))
                .withStatement("INSERT INTO TEST (TEXTVALUE) VALUES (?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {

                                                 private static final long serialVersionUID = 1L;

                                                 public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                                                     logger.info("DB Opertaion json data: " + element);
                                                     System.out.println(element);
                                                     preparedStatement.setString(1, element);
                                                 }
                                             }
                ));
        pipeline.run();
        LOGGER.debug("All done!!");
    }
}

