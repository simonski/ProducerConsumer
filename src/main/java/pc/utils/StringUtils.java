package pc.utils;

/**
 * helper class provides String handling utilities methods
 */
public final class StringUtils {

	public static boolean isNullOrEmpty(String... in) {
		for (String s : in) {
			if (s == null)
				return true;
			if (s.trim().equals(""))
				return true;
		}
		return false;
	}

	public static final long ONE_SECOND = 1000;
	public static final long ONE_MINUTE = ONE_SECOND * 60;
	public static final long ONE_HOUR = ONE_MINUTE * 60;
	public static final long ONE_DAY = ONE_HOUR * 24;

	public static String friendlyTime(long durationInMillis) {

		long[] results = friendlyTimeUnformatted(durationInMillis);

		long days = results[0];
		long hours = results[1];
		long minutes = results[2];
		long seconds = results[3];
		long ms = results[4];

		boolean comma = false;
		StringBuilder sb = new StringBuilder();
		if (days > 0) {
			sb.append(days + " days");
			comma = true;
		}
		if (hours > 0) {
			if (comma) {
				sb.append(", ");
			}
			sb.append(hours + " hours");
			comma = true;
		}

		if (minutes > 0) {
			if (comma) {
				sb.append(", ");
			}
			sb.append(minutes + " minutes");
			comma = true;
		}

		if (seconds > 0) {
			if (comma) {
				sb.append(", ");
			}
			sb.append(seconds + " seconds");
			comma = true;
		}

		if (ms > 0) {
			if (comma) {
				sb.append(", ");
			}
			sb.append(ms + " ms");
		}

		return sb.toString();

	}

	/**
	 * returns long[] as [years,days,hours,minutes,seconds,millis]
	 * 
	 * @param durationInMillis
	 * @return
	 */
	public static long[] friendlyTimeUnformatted(long durationInMillis) {

		long days = 0;
		long hours = 0;
		long minutes = 0;
		long seconds = 0;
		long remaining = durationInMillis;

		if (remaining >= ONE_DAY) {
			days = remaining / ONE_DAY;
			remaining = remaining % ONE_DAY;
		}

		if (remaining >= ONE_HOUR) {
			hours = remaining / ONE_HOUR;
			remaining = remaining % ONE_HOUR;
		}

		if (remaining >= ONE_MINUTE) {
			minutes = remaining / ONE_MINUTE;
			remaining = remaining % ONE_MINUTE;
		}

		if (remaining >= ONE_SECOND) {
			minutes = remaining / ONE_SECOND;
			remaining = remaining % ONE_SECOND;
		}

		return new long[] { days, hours, minutes, seconds, remaining };

	}

}
