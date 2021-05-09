package pc;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import pc.utils.CLI;

public abstract class Consumer implements Runnable {

	public Context context;
	Queue<Record> queue;
	boolean isIdle = false;
	boolean quit = false;
	boolean isComplete = false;
	String name;
	public CLI cli;
	public int writeBuffer = 8 * 1024;

	String delimiter;
	String recordname;
	String namespace;
	public File file;
	int rowCount = 0;

	public int total_success = 0;
	public int total_fails = 0;

	public Consumer() {
		this.queue = new ConcurrentLinkedQueue<Record>();
	}

	public abstract String getOutputFileSuffix();

	public void setup(CLI cli, File outputDir, Context context, int index, int writeBufferSize) throws IOException {
		this.writeBuffer = writeBufferSize;
		this.file = new File(outputDir + "/output-" + index + "." + getOutputFileSuffix());
		this.cli = cli;
		this.name = file.getName();
		this.isIdle = true;
		this.context = context;
		buildWriter();
	}

	public abstract void buildWriter() throws IOException;

	public final void run() {
		try {
			while (!quit) {
				Record t = queue.poll();
				if (t != null) {
					this.isIdle = false;
					if (process(t)) {
						total_success += 1;
					} else {
						total_fails += 1;
					}
					this.isIdle = true;
				}
			}
		} catch (Exception e) {
			this.isIdle = true;
			e.printStackTrace();
		}
		this.isIdle = true;
		isComplete = true;
	}

	public final boolean process(Record line) throws IOException {
		write(line);
		return true;
	}

	public abstract void write(Record record) throws IOException;

	public abstract void close() throws IOException;

}
