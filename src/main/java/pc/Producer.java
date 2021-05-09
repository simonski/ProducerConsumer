package pc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import pc.utils.FileUtils;

public class Producer implements Runnable {

	public Context context;
	public Consumer[] consumers_array;
	public boolean isComplete = false;
	public int row = 0;
	public int readBufferSize;
	public int maxQ;
	public String encoding;

	public Producer() {
	}

	public void run() {
		BufferedReader reader = null;
		consumers_array = new Consumer[context.getConsumers().size()];
		for (int index = 0; index < context.getConsumers().size(); index++) {
			consumers_array[index] = context.getConsumers().get(index);
		}
		try {
			// UTF-8 faster
			// threads 3, buffer 131072 -
			// threads 3, buffer 131072 -
			// 236k read windows :
			// 183k read macOS Intel :
			// ???? read macOS M1 :
			
			GZIPInputStream in = new GZIPInputStream(new FileInputStream(context.getSourceFile()), getReadBufferSize());
			reader = new BufferedReader(new InputStreamReader(in, getEncoding()), getReadBufferSize());

			int index = 0;
			row = 0;
			String line = reader.readLine();
			Consumer consumer = null;
			long startTime = System.currentTimeMillis();
			long maxQ = context.getMaxQ();
			while (line != null) {
				consumer = consumers_array[index];
				Record r = new Record(line,row);
				consumer.queue.add(r);
				line = reader.readLine();
				index += 1;
				index = index % consumers_array.length;
				row += 1;

				// bit of defence as if the overall queue size gets too large
				// we GC and stop the world
				// this comes at low-to-no hit
				if (row % 1000 == 0 ) {
					try {
						long queueSize = getTotalQueueSize();
						while (getTotalQueueSize() > maxQ ) {
							context.report.out("Producer: Sleeping. ( queue size is " + queueSize + ", maxQ is " + maxQ);
							// stop reading for a while
							Thread.sleep(10);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			}
			context.report.metric("rows", row);
			context.report.metric("total_read_time_ms", System.currentTimeMillis()-startTime);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			FileUtils.close(reader);
			isComplete = true;
		}
		
		
	}

	public long getTotalQueueSize() {
		long current_q = 0;
		for (int index=0; index<consumers_array.length; index++) {
			current_q += consumers_array[index].queue.size();
		}
		return current_q;
	}

	public int getReadBufferSize() {
		return readBufferSize;
	}

	public void setReadBufferSize(int readBufferSize) {
		this.readBufferSize = readBufferSize;
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}


	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getEncoding() {
		return encoding;
	}


}
