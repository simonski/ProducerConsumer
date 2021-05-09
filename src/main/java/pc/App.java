package pc;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import impl.CSV;
import impl.Empty;
import pc.utils.CLI;

public class App {

	CLI cli;        // options passed via terminal
	Context context; // the shared state between producer/consumers

	public static void main(String[] args) throws Exception {
		CLI cli = new CLI(args);
		App app = new App(cli);
		if (cli.args.length == 0) {
			app.usage();
		} else {
			app.execute();
		}
	}

	public static void help() {
		CLI.out("pc reads and converts a gzip, quickly");
		CLI.out("Required parameters");
		CLI.out("");
		CLI.out(" -source      FILE          file gzip file to convert");
		CLI.out(" -target      PATH          the path to store the converted files");
		CLI.out(" -consumer    class.Name    worker implementation class (e.g. 'csv, 'empty')");
		CLI.out("");
		CLI.out("Optional parameters");
		CLI.out("");
		CLI.out(" -threads     N             set number of workers");
		CLI.out(" -readBuffer  N             read buffer size in k (default 8)");
		CLI.out(" -writeBuffer N             write buffer size in k (default 8)");
		CLI.out(" -dir         DIR           the directory to store data during conversion");
		CLI.out("");
		CLI.out(" -rows        N             process only N rows");
		CLI.out(" -rowcount    N             number of rows in source data (for ETA reporting)");
		CLI.out(" -report      N             report every N milliseconds on progress (default 1000)");
		CLI.out(" -maxq        N             maximum allowed queue size before the reader will pause");
		CLI.out("");
	}

	public App(CLI cli) throws Exception {
		this.cli = cli;
		if (cli.contains("-help") || cli.contains("-h") || cli.args.length < 3) {
			help();
			System.exit(1);
		}
		boolean quiet = cli.contains("-quiet");
		context = new Context();
		context.report = new Report(!quiet);
		context.sourceFile = cli.getFileOrDie("-source");
		context.targetDir = new File(cli.getStringOrDie("-target"));
		context.targetDir.mkdirs();
		context.maxQ = cli.getIntOrDefault("-maxq", 1000000);
		context.reportEveryMS = cli.getIntOrDefault("-report", 1000);
		
		context.consumers = buildConsumers(context, cli);
		context.producer = buildProducer(context, cli);
	}

	public Producer buildProducer(Context context, CLI cli) throws Exception {
		int readBufferSize = 1024 * cli.getIntOrDefault("-readBuffer", 8);
		String defaultClassName = Producer.class.getName();
		String className = cli.getStringOrDefault("-producer", defaultClassName);
		String encoding = cli.getStringOrDefault("-encoding", "UTF-8");
		Producer p = (Producer) Class.forName(className).newInstance();
		p.setContext(this.context);
		p.setEncoding(encoding);
		p.setReadBufferSize(readBufferSize);
		return p;
	}

	public List<Consumer> buildConsumers(Context context, CLI cli) throws Exception {
		Report report = context.report;
		String className = cli.getStringOrDie("-consumer");
		int writeBufferSize = 1024 * cli.getIntOrDefault("-writeBuffer", 8);
		int maxAllowedConsumers = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
		int numberOfConsumers = cli.getIntOrDefault("-threads", maxAllowedConsumers); // number of workers to run on
		List<Consumer> consumers = new ArrayList<Consumer>();

		if (numberOfConsumers < 1 || numberOfConsumers > maxAllowedConsumers) {
			numberOfConsumers = maxAllowedConsumers;
		}
		report.out("Using " + numberOfConsumers + " threads (max available is " + maxAllowedConsumers + ").");
		report.out("Using max queue size " + context.maxQ);
		context.threads = numberOfConsumers + 1;
		context.threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(context.threads);
		for (int i = 0; i < numberOfConsumers; i++) {
			if ("csv".equalsIgnoreCase(className)) {
				className = CSV.class.getName();
			} else if ("empty".equalsIgnoreCase(className)) {
				className = Empty.class.getName();
			}
			Consumer consumer = (Consumer) Class.forName(className).newInstance();
			consumer.setup(cli, context.targetDir, context, i, writeBufferSize);
			consumers.add(consumer);
		}

		return consumers;
	}

	public void usage() {
		CLI.out("pc: usage");
	}

	public void execute() {

		Report report = context.report;
		context.timeStarted = System.currentTimeMillis();
		report.metric("start", context.timeStarted);

		// start the workers
		for (Consumer worker : context.consumers) {
			context.threadPool.execute(worker);
		}
		// start pushing work into each one
		context.threadPool.execute(context.producer);

		// wait until they are complete
		boolean quit = false;
		while (!quit) {
			if (context.producer.isComplete) {
				// default to quitting as we think all work is complete
				quit = true;
				for (Consumer worker : context.consumers) {

					if (worker.quit == false) {
						if (worker.queue.size() > 0 || !worker.isIdle) {
							// the worker still has work to do
							quit = false;
							break;
						} else {
							// the worker does not have work to do
							worker.quit = true;
							try {
								worker.close();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			}

			if (!quit) {
				try {
					Thread.sleep(context.reportEveryMS);
					report();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		// every consumer has completed
		// every producer has completed
		// now we may need to copy the files to some remote storage
		context.threadPool.shutdown();

		long rowsWrittenPerSecond = finalReport();
		long end = System.currentTimeMillis();
		report.metric("end", end);
		report.metric("duration", end - context.timeStarted);
		report.metric("rows_per_second", rowsWrittenPerSecond);
		report.config("source", context.sourceFile.toString());
		report.config("target", context.targetDir.toString());
		report.config("consumer", cli.getStringOrDefault("-consumer", ""));
		report.config("threads", context.threads);

		File reportFile = new File(context.targetDir + "/report.json");
		try {
			report.save(reportFile);

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	long finalReport() {

		context.timeThisSplit = System.currentTimeMillis();
		int totalSuccess = 0;
		int totalFails = 0;
		int totalProcessed = 0;
		long queueSize = 0;
		for (Consumer worker : context.consumers) {
			totalSuccess += worker.total_success;
			totalFails += worker.total_fails;
			totalProcessed = totalSuccess + totalFails;
			queueSize += worker.queue.size();
		}
		long duration = context.timeThisSplit - context.timeStarted;
		float rowsReadPerMS = context.producer.row / duration;
		float rowsWrittenPerMS = totalProcessed / duration;
		long rowsWrittenPerS = (int) (rowsWrittenPerMS * 1000);
		long rowsReadPerS = (int) (rowsReadPerMS * 1000);
//		String actualWritten = StringUtils.friendlyTime((timeThisSplit - timeStarted));
//		String actualRead = StringUtils.friendlyTime((timeThisSplit - timeStarted));
		context.report.out(rowsWrittenPerS + " write/s, " + rowsReadPerS + " read/s.");
		return rowsWrittenPerS;
	}

	void report() {
		context.timeThisSplit = System.currentTimeMillis();
		int totalSuccess = 0;
		int totalFails = 0;
		int totalProcessed = 0;
		int totalRead = context.producer.row;
		long queueSize = 0;
		for (Consumer worker : context.consumers) {
			totalSuccess += worker.total_success;
			totalFails += worker.total_fails;
			totalProcessed = totalSuccess + totalFails;
			queueSize += worker.queue.size();
		}

		long duration = context.timeThisSplit - context.timeStarted;
		float rowsWrittenPerMS = totalProcessed / duration;
		long rowsWrittenPerS = (int) (rowsWrittenPerMS * 1000);

		float rowsReadPerMS = totalRead / duration;
		long rowsReadPerS = (int) (rowsReadPerMS * 1000);

		// String actual = StringUtils.friendlyTime(duration);
		String actual = duration / 1000 + "s";

		context.report.out(totalProcessed + " (" + totalFails + " fails.) (" + rowsWrittenPerS + "row writes/second, "
				+ rowsReadPerS + " row reads/second), actual " + actual + ", queue size " + queueSize
				+ ", map size " + context.map.size());

		context.timeLastSplit = context.timeThisSplit;
	}

}
