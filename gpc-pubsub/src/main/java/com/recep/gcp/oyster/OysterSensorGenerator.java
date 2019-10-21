package com.recep.gcp.oyster;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OysterSensorGenerator {
	private static final Logger logger = LogManager.getLogger(OysterSensorGenerator.class);
	private List<Station> stations = new ArrayList<Station>();

	private int min = 77777777;
	private int max = 99999999;
	private Random random = new Random();
	private DateTimeFormatter format = DateTimeFormatter.ofPattern("dd/mm/yyyy hh:mm:ss");

	private final static int MAX_PASSENGER_PER_STATION = 2;

	public OysterSensorGenerator(List stations) {
		this.stations = stations;
	}

	public List<String> generateSensorData() {

		List<String> data = new ArrayList<String>();
		for (Station st : stations) {
			if (random.nextBoolean()) {
				for (int i = 0; i < random.nextInt(MAX_PASSENGER_PER_STATION + 1); i++) {
					String s = produceSensorData(st);
					data.add(s);
					logger.debug(s);
				}
			}
		}

		return new ArrayList<String>(data);
	}

	public String produceSensorData(Station station) {
		StringBuilder data = new StringBuilder();
		data.append(LocalDateTime.now().format(format)).append(",");
		data.append(station.name).append(",");
		data.append(pickRandomGate(station.numberOfGate)).append(",");
		data.append(generateOysterNo()).append(",");
		data.append(random.nextInt(2));
		data.append(",").append(LocalDateTime.now().toString());
		return data.toString();
	}

	private int generateOysterNo() {
		return random.ints(min, (max + 1)).findFirst().getAsInt();
	}

	private int pickRandomGate(int totalOysterGate) {
		return random.ints(1, totalOysterGate + 1).findFirst().getAsInt();
	}

}
