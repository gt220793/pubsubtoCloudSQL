package com.journaldev.bean;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import autovalue.shaded.com.google$.common.collect.$ForwardingList;
import joptsimple.internal.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class PipeLineTest {

    /**
     * The Constant LOGGER.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(PipeLineTest.class);

    /**
     * The Constant INPUT_FILE_NAME.
     */
    private static final String INPUT_FILE_NAME = "gs://pubsub-json-demo/sample_json.txt";

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
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());
        File file = new File(INPUT_FILE_NAME);
        PCollection<String> input = pipeline.apply(TextIO.read().from(INPUT_FILE_NAME));
		 //  PCollection<String> input = pipeline.apply(file.read().from(INPUT_FILE_NAME));
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
				.withStatement("INSERT INTO JSON (complete_value)VALUES (?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {

                    private static final long serialVersionUID = 1L;

                    public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
                        logger.info("DB Opertaion xml data: " + element);
                        System.out.println(element);
                        preparedStatement.setString(1, element);
                    }
                }
				));
		pipeline.run();
		LOGGER.debug("All done!!");
    }
}

