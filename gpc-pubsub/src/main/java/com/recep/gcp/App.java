package com.recep.gcp;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import com.google.api.client.util.Sleeper;
import com.recep.gcp.oyster.OysterSensorGenerator;
import com.recep.gcp.oyster.Station;
import com.recep.gcp.pubsub.BasicPublisher;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class App {
	private static final Logger logger = LogManager.getLogger(App.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		String topicId = args[0];

		Random r = new Random();

		System.out.println("logger.isInfoEnabled() : " + logger.getLevel());

		List<Station> stations = new ArrayList<Station>();

		stations.add(new Station("Richmond", 10));
		stations.add(new Station("Kew Gardens", 8));
		stations.add(new Station("Hammersmith", 15));

		OysterSensorGenerator senseorGenerator = new OysterSensorGenerator(stations);

		BasicPublisher publisher = new BasicPublisher(topicId);
		List<String> delayQueue = new ArrayList<String>();
		int delayQueueCycle =0;
		try {
			while (true) {
				List<String> messages = senseorGenerator.generateSensorData();
				logger.info("Number of {} message generated.. DelayQueue Cycle :{}. DelayQueue Size : {}.", messages.size(), delayQueueCycle, delayQueue.size());

				try {
					int i = r.nextInt(20);
					if (i == 7) {
						logger.info("Going for a long sleep");
						Thread.sleep(2000);
					} else if ( i > 18) {  //delay queue
						logger.info("Entering DelayQueue. {} Number of messages goes into delayQueue.." , messages.size());
						delayQueue.addAll(messages);
						delayQueueCycle++;
					} else { //sleep
						Thread.sleep(r.nextInt(100));
					}

				} catch (InterruptedException e) {
					logger.error(e);
					e.printStackTrace();
				}
				
				if(delayQueueCycle > 4) {
					logger.info("Flushing delayQueue. Cycle is {}. Size of delayQueue is  {}.",delayQueueCycle, delayQueue.size());
					publisher.sendMessages(delayQueue);
					delayQueue.clear();
					delayQueueCycle=0;
					logger.info("delayQueue are flushed.Size of delayQueue is  {}.", delayQueue.size());
				}
				
				publisher.sendMessages(messages);

			}
		} finally {
			logger.error("Bye bye bye.....");
			publisher.shutdown();
		}

	}

}
