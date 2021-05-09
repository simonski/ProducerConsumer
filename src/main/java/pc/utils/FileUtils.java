package pc.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

public class FileUtils {

	public static final int ONE_K = 1024;
	public static final int FOUR_K = ONE_K * 4;
	public static final long ONE_MEGABYTE = ONE_K * 1024;
	public static final long ONE_GIGABYTE = ONE_MEGABYTE * 1024;
	public static final long ONE_TERABYTE = ONE_GIGABYTE * 1024;
	public static final long ONE_PETABYTE = ONE_TERABYTE * 1024;

//
//	static Properties mimeTypes;
//	static Set<String> ASCII;
//
//	static {
//		mimeTypes = new Properties();
//		mimeTypes.setProperty("zip", "application/zip");
//		mimeTypes.setProperty("xml", "application/xml");
//		mimeTypes.setProperty("xsl", "application/xml");
//		mimeTypes.setProperty("html", "text/html");
//		mimeTypes.setProperty("js", "text/javascript");
//		mimeTypes.setProperty("json", "text/javascript");
//		mimeTypes.setProperty("htm", "text/html");
//		mimeTypes.setProperty("css", "text/css");
//		mimeTypes.setProperty("txt", "text/plain");
//		mimeTypes.setProperty("png", "image/png");
//		mimeTypes.setProperty("jpeg", "image/jpeg");
//		mimeTypes.setProperty("jpg", "image/jpeg");
//		mimeTypes.setProperty("gif", "image/gif");
//		mimeTypes.setProperty("svg", "image/svg+xml");
//		mimeTypes.setProperty("wav", "audio/x-wav");
//		mimeTypes.setProperty("abs", "audio/x-mpeg");
//		mimeTypes.setProperty("aif", "audio/x-aiff");
//		mimeTypes.setProperty("au", "audio/basic");
//		mimeTypes.setProperty("m3u", "audio/x-mpegurl");
//		mimeTypes.setProperty("mp3", "audio/mpeg");
//		mimeTypes.setProperty("m4a", "audio/mp4a-latm");
//
//		mimeTypes.setProperty("aif", "audio/x-aiff");
//		mimeTypes.setProperty("aifc", "audio/x-aiff");
//		mimeTypes.setProperty("aiff", "audio/x-aiff");
//
//		mimeTypes.setProperty("au", "audio/basic");
//		mimeTypes.setProperty("avi", "video/x-msvideo");
//
//		mimeTypes.setProperty("mid", "audio/midi");
//		mimeTypes.setProperty("midi", "audio/midi");
//		mimeTypes.setProperty("mif", "application/vnd.mif");
//		mimeTypes.setProperty("mov", "video/quicktime");
//		mimeTypes.setProperty("movie", "video/x-sgi-movie");
//		mimeTypes.setProperty("mp2", "audio/mpeg");
//		mimeTypes.setProperty("mp3", "audio/mpeg");
//		mimeTypes.setProperty("mp4", "video/mp4");
//		mimeTypes.setProperty("mpe", "video/mpeg");
//		mimeTypes.setProperty("mpeg", "video/mpeg");
//		mimeTypes.setProperty("mpg", "video/mpeg");
//		mimeTypes.setProperty("mpga", "audio/mpeg");
//		mimeTypes.setProperty("ms", "application/x-troff-ms");
//		mimeTypes.setProperty("msh", "model/mesh");
//		mimeTypes.setProperty("mxu", "video/vnd.mpegurl");
//		mimeTypes.setProperty("nc", "application/x-netcdf");
//		mimeTypes.setProperty("oda", "application/oda");
//		mimeTypes.setProperty("ogg", "application/ogg");
//
//		mimeTypes.setProperty("qt", "video/quicktime");
//		mimeTypes.setProperty("qti", "image/x-quicktime");
//		mimeTypes.setProperty("qtif", "image/x-quicktime");
//		mimeTypes.setProperty("ra", "audio/x-pn-realaudio");
//		mimeTypes.setProperty("ram", "audio/x-pn-realaudio");
//
//		ASCII = new HashSet<String>();
//		ASCII.add("xml");
//		ASCII.add("xsl");
//		ASCII.add("html");
//		ASCII.add("htm");
//		ASCII.add("css");
//		ASCII.add("svg");
//		ASCII.add("txt");
//		ASCII.add("xhtml");
//		ASCII.add("js");
//		ASCII.add("json");
//
//	}
//
	/**
	 * indicates IO progress; the callback should return FALSE to cancel the writing
	 * 
	 * @author simon
	 */
	public interface Callback {
		/**
		 * writing has occurred
		 * 
		 * @param totalBytesSoFar
		 * @return boolean indicating quit status - quit will stop downloading and
		 *         onCancelled will be invoked
		 */
		public boolean onProgress(long totalBytesSoFar);

		/**
		 * writing is complete
		 */
		public void onComplete();

		/**
		 * writing was cancelled at the current bytes so far stage
		 * 
		 * @param totalBytesSoFar
		 */
		public void onCancelled(long totalBytesSoFar);

		/**
		 * when an error occurs, this will be triggered
		 * 
		 * @param e
		 */
		public void onException(Exception e);

		public static final Callback EMPTY_IMPL = new Callback() {

			public boolean onProgress(long totalBytesSoFar) {
				return false;
			}

			public void onException(Exception e) {
			}

			public void onComplete() {
			}

			public void onCancelled(long totalBytesSoFar) {
			}

		};

	}

	/**
	 * resolves the passed fileName to a File, using the passed 'currentDir' if
	 * necessary, replacing wildcards where possible, e.g. ~ =
	 * System.getProperty("user.home")
	 * 
	 * @param fileName
	 * @return
	 */
	public static File resolveFile(File currentDir, String fileName) {
		fileName = fileName.replaceAll("~", System.getProperty("user.home"));
		return new File(currentDir + File.separator + fileName);
	}

	/**
	 * resolves the passed fileName to a File, replacing wildcards where possible,
	 * e.g. ~ = System.getProperty("user.home")
	 * 
	 * @param fileName
	 * @return
	 */
	public static File resolveFile(String fileName) {
		if (fileName == null || "".equals(fileName.trim()))
			return null;
		fileName = fileName.replaceAll("~", System.getProperty("user.home"));
		File f = new File(fileName);
		if (fileName.contains("..")) {
			try {
				fileName = f.getCanonicalPath();
				f = new File(fileName);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return f;
	}

	public static int writeStream(InputStream in, OutputStream out) throws IOException {
		return writeStream(in, out, null);
	}

	public static int writeStream(InputStream in, OutputStream out, Callback callback) throws IOException {

		int one_k = 1024;
		int buffer_size = one_k * 10;
		byte[] byteBuffer = new byte[buffer_size];
		int count = 0;
		int read = 0;

		if (callback == null) {
			callback = new Callback() {

				public boolean onProgress(long totalBytesSoFar) {
					// TODO Auto-generated method stub
					return false;
				}

				public void onComplete() {
					// TODO Auto-generated method stub

				}

				public void onCancelled(long totalBytesSoFar) {
					// TODO Auto-generated method stub

				}

				public void onException(Exception e) {
					// TODO Auto-generated method stub

				}

			};

		}

		while ((count = in.read(byteBuffer, 0, buffer_size)) != -1) {
			out.write(byteBuffer, 0, count);
			read += count;
			boolean quit = false; // callback.onProgress(read);
			if (quit) {
				callback.onCancelled(read);
				break;
			}
		}
		out.flush();
		out.close();
		callback.onComplete();

		return read;

	}

	public static void close(InputStream in) {
		try {
			in.close();
		} catch (Exception e) {

		}
	}

	public static void close(Reader reader) {
		try {
			reader.close();
		} catch (Exception e) {

		}
	}

	public static void close(OutputStream out) {
		try {
			out.close();
		} catch (Exception e) {

		}
	}

	public static void close(Writer writer) {
		try {
			writer.close();
		} catch (Exception e) {

		}
	}

	public static byte[] readFileToBytes(File file) throws IOException {

		ByteArrayOutputStream bos = null;
		FileInputStream fis = null;
		try {
			bos = new ByteArrayOutputStream();
			fis = new FileInputStream(file);
			FileUtils.writeStream(fis, bos, null);
			return bos.toByteArray();
		} finally {
			bos.close();
			fis.close();
		}

	}

	public static StringBuffer readFileToStringBuffer(File file) throws IOException {
		BufferedReader reader = null;
		if (file.exists() == false) {
			return new StringBuffer();
		}
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = reader.readLine();
			}
			reader.close();
			return sb;
		} finally {
			try {
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * returns only the filename in a string /a/b/c/d/e.csv returns e.csv returns
	 * only the filename in a string gs://a/b/c/d/e.csv returns e.csv returns only
	 * the filename in a string s3://a/b/c/d/e.csv returns e.csv
	 * 
	 * @param path
	 * @return
	 */
	public static String getFilename(String path) {
		String[] splits = path.split(File.separator);
		return splits[splits.length - 1];
	}

}
