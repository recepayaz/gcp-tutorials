/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.recep.gcp;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import com.recep.gcp.OysterSensorData.Field;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class StarterPipeline {
	private static final Logger logger = LoggerFactory.getLogger(StarterPipeline.class);

	public static interface MyOptions extends DataflowPipelineOptions {
		@Description("Also stream to Bigtable?")
		@Default.Boolean(false)
		boolean getBigtable();

		void setBigtable(boolean b);
	}

	public static class ConvertToBQRow extends DoFn<PubsubMessage, TableRow> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			logger.info("Inside processor... ");
			PubsubMessage msg = c.element();

			String s = new String(msg.getPayload(), StandardCharsets.UTF_8);
			System.out.println("Line: " + s);
			OysterSensorData sensorData = OysterSensorData.Of(s);
			String oysterno = sensorData.getField(Field.OYSTERNO);

			TableRow row = new TableRow();

			row.set("timestamp", sensorData.getField(Field.TIMESTAMP2));
			row.set("station", sensorData.getField(Field.STATION));
			row.set("gateno", sensorData.getField(Field.GATENO));
			row.set("oysterno", sensorData.getField(Field.OYSTERNO));
			row.set("inout", sensorData.getField(Field.INOUT));
			c.output(row);
		}
	}

	public static void main(String[] args) {

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("station").setType("STRING"));
		fields.add(new TableFieldSchema().setName("gateno").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("oysterno").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("inout").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);

		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		System.out.println("Starting ... . ..  .. .   .");
		String topic = "projects/" + options.getProject() + "/topics/my-test-topic";
		String currConditionsTable = options.getProject() + ":demos.oyster_traffic";

		p.apply("GetMessages", PubsubIO.readMessagesWithAttributes().fromTopic(topic))
				.apply("Convert Sensor Data", ParDo.of(new ConvertToBQRow()))
				.apply("Insert", BigQueryIO.writeTableRows().to(currConditionsTable)//
						.withSchema(schema)//
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run();
	}
}
