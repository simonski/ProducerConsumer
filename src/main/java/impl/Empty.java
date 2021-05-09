package impl;

import java.io.IOException;

import pc.Consumer;
import pc.Record;

public class Empty extends Consumer {

	public Empty() {
		super();

	}

	public void buildWriter() throws IOException {
	}

	public void write(Record record) {
		String[] splits = record.line.split(",", -1);
	}

	public void close() throws IOException {
	}

	@Override
	public String getOutputFileSuffix() {
		return "";
	}
}
