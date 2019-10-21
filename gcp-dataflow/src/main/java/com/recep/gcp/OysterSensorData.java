package com.recep.gcp;

public class OysterSensorData {

	private String[] fields;

	public enum Field {
		TIMESTAMP, STATION, GATENO, OYSTERNO, INOUT, TIMESTAMP2;
	}

	public OysterSensorData() {

	}

	private OysterSensorData(String[] fields) {
		this.fields = fields;
	}

	public static OysterSensorData Of(String sensorData) {
		String[] tmp = sensorData.split(",");
		OysterSensorData oyster = new OysterSensorData(tmp);
		return oyster;
	}
	
	public String getField(Field f) {
		return this.fields[f.ordinal()];
	}
}
