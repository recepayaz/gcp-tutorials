package com.recep.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.recep.gcp.App;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicPublisher {
	public Publisher publisher = null;
	public String topicId;
	// use the default project id
	private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

	private static final Logger logger = LogManager.getLogger(BasicPublisher.class);

	public BasicPublisher(String topicId) {
		super();
		this.topicId = topicId;
	}

	private void init() throws IOException {
		if (publisher == null) {
			ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topicId);
			// Create a publisher instance with default settings bound to the topic
			publisher = Publisher.newBuilder(topicName).build();
			logger.info("Building new publisher....................");
		}
	}

	public void shutdown() {
		if (publisher != null) {
			// When finished with the publisher, shutdown to free up resources.
			logger.info("Shutting down....");

			publisher.shutdown();
		}
	}

	public void sendMessages(List<String> messages) throws InterruptedException, ExecutionException, IOException {

		init();
		List<ApiFuture<String>> futures = new ArrayList<ApiFuture<String>>();
		try {
			for (String message : messages) {
				// convert message to bytes
				ByteString data = ByteString.copyFromUtf8(message);
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

				// Schedule a message to be published. Messages are automatically batched.
				ApiFuture<String> future = publisher.publish(pubsubMessage);
				futures.add(future);
			}
			logger.info("{} numberof messages are sent", messages.size());
		} finally {
			// Wait on any pending requests
			List<String> messageIds = ApiFutures.allAsList(futures).get();
			messageIds.forEach(logger::info);
		}

	}

}
