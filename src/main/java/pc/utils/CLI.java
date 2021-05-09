package pc.utils;

import java.io.Console;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class CLI {

	public String[] args;
	public String line;
	public static String PREFIX = "";

	public CLI(String[] args) {
		this.args = args;
		line = "";
		for (String arg : args) {
			line += arg;
			line += " ";
		}
		line = line.trim();
	}

	public CLI(String lineOfArgs) {
		this.line = lineOfArgs;
		this.args = lineOfArgs.split(" ");
	}

	/**
	 * 'fixes' the args so that quoted args become a single entry
	 * 
	 * @param args
	 * @return
	 */
	public static String[] parseArgs(String[] args) {
		StringBuffer sb = new StringBuffer();
		for (String s : args) {
			sb.append(s);
			sb.append(" ");
		}
		StringTokenizer st = new StringTokenizer(sb.toString().trim());
		List<String> argsList = new ArrayList<String>();
		// the command first
		argsList.add(st.nextToken());
		sb = new StringBuffer();
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			if (token.startsWith("-")) {
				String value = sb.toString().trim();
				if (!"".equals(value)) {
					argsList.add(sb.toString().trim());
				}
				argsList.add(token);
				sb = new StringBuffer();
			} else {
				sb.append(token);
				sb.append(" ");
			}
		}
		String value = sb.toString().trim();
		if (!"".equals(value)) {
			argsList.add(sb.toString().trim());
		}
		args = new String[argsList.size()];
		argsList.toArray(args);
		return args;

	}

	public int getIntOrDefault(String key, int defaultValue) {
		if (getStringOrDefault(key, null) == null) {
			return defaultValue;
		} else {
			try {
				int value = Integer.parseInt(getStringOrDefault(key, "" + defaultValue));
				return value;
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	public float getFloatOrDefault(String key, float defaultValue) {
		if (getStringOrDefault(key, null) == null) {
			return defaultValue;
		} else {
			try {
				float value = Float.parseFloat(getStringOrDefault(key, "" + defaultValue));
				return value;
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	public int getIntOrDie(String key) {
		if (getStringOrDefault(key, null) == null) {
			die(key + " is required.");
			return -1;
		} else {
			try {
				int value = Integer.parseInt(getStringOrDefault(key, null));
				return value;
			} catch (Exception e) {
				die(key + " is required.");
				return -1;
			}
		}
	}

	public int indexOf(String key) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				return i;
			}
		}
		return -1;
	}

	public boolean contains(String entry) {
		return indexOf(entry) > -1;
	}

	public String getStringOrDefault(String key, String defaultValue) {
		int index = indexOf(key);
		if (index == -1) {
			return defaultValue;
		} else if (args.length > index + 1) {
			String value = args[index + 1];
			if (isKey(value)) {
				return defaultValue;
			} else {
				return value;

			}
		} else {
			return defaultValue;
		}
	}

	public boolean getBooleanOrDefault(String key, boolean defaultValue) {
		String var = getStringOrDefault(key, null);
		if (var == null) {
			return defaultValue;
		} else {
			try {
				return Boolean.parseBoolean(var);
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	public boolean isKey(String key) {
		return key != null && key.startsWith("-");
	}

	public boolean isVerbose() {
		return indexOf("-v") > -1;
	}

	/**
	 * returns the string of all values starting at the passed index - stops when it
	 * encounters a key
	 * 
	 * @param index
	 * @return
	 */
	public String getAllValuesFrom(int index) {
		String value = "";
		for (int i = index; i < args.length; i++) {
			if (args[i].startsWith("-")) {
				break;
			} else {
				value += args[i];
				value += " ";
			}
		}
		return value.trim();
	}

	public String getAllValuesFromIndexOrDefault(int index, String defaultValue) {
		String value = "";
		for (int i = index; i < args.length; i++) {
			if (args[i].startsWith("-")) {
				break;
			} else {
				value += args[i];
				value += " ";
			}
		}
		if (value.equals("")) {
			return defaultValue;
		} else {
			return value.trim();
		}
	}

	public String toString(int fromIndex) {
		StringBuffer sb = new StringBuffer();
		for (int index = fromIndex; index < args.length; index++) {
			sb.append(args[index]);
			sb.append(" ");
		}
		return sb.toString().trim();
	}

	public String toString(int fromIndex, int toIndex) {
		StringBuffer sb = new StringBuffer();
		for (int index = fromIndex; index <= toIndex; index++) {
			sb.append(args[index]);
			sb.append(" ");
		}
		return sb.toString().trim();
	}

	@Override
	public String toString() {
		return toString(0);
	}

	public String getStringOrDie(String key) {
		int index = indexOf(key);
		if (index == -1) {
			die(key + " is required.");
			return null;
		}
		String result = args[index + 1];
		if (result.startsWith("-")) {
			die(key + " is required.");
			return null;
		}
		return result;
	}

	public File getFileOrDie(String key) {
		if (indexOf(key) == -1) {
			die("'" + key + "' is required.");
		} else {
			String str = getStringOrDefault(key, null);
			if (StringUtils.isNullOrEmpty(str)) {
				die("'" + key + "' is required.");
			}
			File file = FileUtils.resolveFile(str);
			if (file.exists()) {
				return file;
			} else {
				die(file + " does not exist.");
			}
		}
		return null;
	}

	public File getFileOrDefault(String key, File defaultFile) {
		if (indexOf(key) == -1) {
			return defaultFile;
		} else {
			String str = getStringOrDefault(key, null);
			if (str != null) {
				File file = FileUtils.resolveFile(str);
				return file;
			} else {
				return defaultFile;
			}
		}
	}

	public File getDirOrDie(String key) {
		File file = getFileOrDie(key);
		if (file.isDirectory()) {
			return file;
		} else {
			die(file + " is not a directory.");
		}
		return null;
	}

	public static void die(String msg) {
		System.err.println(PREFIX + msg);
		System.exit(1);
	}

	public static void die(String msg, int exitCode) {
		System.err.println(PREFIX + msg);
		System.exit(exitCode);
	}

	public static void die(Exception e) {
		e.printStackTrace();
		System.exit(1);
	}

	public static void die(Exception e, int exitCode) {
		e.printStackTrace();
		System.exit(exitCode);
	}

	public static void out(String msg) {
		System.out.println(PREFIX + msg);
	}

	public static void err(String msg) {
		System.err.println(PREFIX + msg);
	}

	public static void err(String msg, Exception e) {
		System.err.println(PREFIX + msg);
		e.printStackTrace();
	}

	public static void printf(String format, Object... args) {
		System.out.printf(PREFIX + format, args);
	}

	public static String readLine(String fmt, Object... args) {
		Console c = System.console();
		return c.readLine(fmt, args);
	}

	public static char[] readPassword(String fmt, Object... args) {
		Console c = System.console();
		return c.readPassword(fmt, args);
	}

	public static char[] readPassword() {
		Console c = System.console();
		return c.readPassword();
	}

	public static String flattenArray(String[] array, int startAt, String delimeter) {
		StringBuffer sb = new StringBuffer();
		for (int i = startAt; i < array.length; i++) {
			sb.append(array[i]);
			if (i < array.length - 1) {
				sb.append(delimeter);
			}
		}
		return sb.toString();
	}

}
