package pc;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Intended to hold shared state between Consumers
 *
 */
public class Context {

	public ConcurrentHashMap<String, AtomicLong> map;
	
	public Report report;
	public Producer producer;        // the reader
	public List<Consumer> consumers; // the writers

	public File sourceFile;
	public File targetDir;		// where we write to

	public ThreadPoolExecutor threadPool;
	public int threads; // the number of threads in the pool
	public int reportEveryMS; // delay between reporting in millis
	public int maxQ;				// maximum size a queu can be before we wait to write to it

	public long timeStarted = System.currentTimeMillis();
	public long timeLastSplit = System.currentTimeMillis();
	public long timeThisSplit = System.currentTimeMillis();

	public Context() {
		map = new ConcurrentHashMap<String, AtomicLong>();
	}

	public List<Consumer> getConsumers() {
		return consumers;
	}

	public File getSourceFile() {
		return sourceFile;
	}
	
	public int getMaxQ() {
		return maxQ;
	}

	public void setMaxQ(int maxq) {
		this.maxQ = maxq;
	}
	

}
