package pc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONObject;

import pc.utils.CLI;

public class Report {

	long lastCall = 0;
	JSONObject data;
	JSONArray log;
	JSONObject metrics;
	JSONObject config;
	JSONObject facts;
	boolean printToSTDOUT = true;

	public Report(boolean printToSTDOUT) {
		this.printToSTDOUT = printToSTDOUT;
		data = new JSONObject();
		log = new JSONArray();
		metrics = new JSONObject();
		facts = new JSONObject();
		config = new JSONObject();
		data.put("log", log);
		data.put("metrics", metrics);
		data.put("facts", facts);
		data.put("config", config);
	}

	public void metric(String key, long value) {
		metrics.put(key, value);
	}

	public void fact(String key, long value) {
		facts.put(key, value);
	}

	public void fact(String key, String value) {
		facts.put(key, value);
	}

	public void config(String key, long value) {
		config.put(key, value);
	}

	public void config(String key, String value) {
		config.put(key, value);
	}

	public void out(String msg) {
		lastCall = System.currentTimeMillis();
		JSONObject entry = new JSONObject();
		entry.put("time", lastCall);
		entry.put("message", msg);
		log.put(entry);
		if (printToSTDOUT) {
			CLI.out(msg);
		}
	}

	public void out(String msg, Exception e) {
		lastCall = System.currentTimeMillis();
		if (printToSTDOUT) {
			CLI.out(msg);
		}
	}

	public long timeSinceLast() {
		return System.currentTimeMillis() - lastCall;
	}

	public void save(File file) throws IOException {
		FileWriter writer = new FileWriter(file);
		String content = data.toString(4);
		writer.write(content);
		writer.close();
	}

}
