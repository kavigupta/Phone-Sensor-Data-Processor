package dataprocessor.main;

import static dataprocessor.data.manipulation.SpreadsheetHandler.mergeByIndex;
import static dataprocessor.data.manipulation.SpreadsheetHandler.modifyColumn;
import static dataprocessor.data.manipulation.SpreadsheetHandler.pullFirst;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import dataprocessor.io.IO;
import dataprocessor.struct.Pair;

public class Main_SensorDataProcessor {
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		processAccGyrMagData(
				"/home/harveylu/workspace/summer/Calibration-Sensor-Microcontroller/data/2015-07-08-minglei",
				",", 1000);
	}
	/**
	 * Processes a folder containing acceleration, gyroscope, and magnetometer
	 * data. The data must be as follows
	 * 
	 * <pre>
	 * {dir}/mag.csv
	 * {dir}/gyr.csv
	 * {dir}/acc.csv
	 * </pre>
	 * 
	 * where all files have headers and contain the columns {@code Time x y z}
	 * with any number of additional columns optional.
	 * 
	 * The {@code separator} variable is used to contain the cell separator the
	 * file uses.
	 * 
	 * This program will produce an output file at
	 * 
	 * <pre>
	 * {dir}/human-readable.csv
	 * </pre>
	 * 
	 * for human processing and an output file at
	 * 
	 * <pre>
	 * {dir}/C-readable.csv
	 * </pre>
	 * 
	 * for the C program
	 */
	public static void processAccGyrMagData(String dir, String separator,
			int rows) throws FileNotFoundException, IOException {
		Function<Pair<String, String>, Optional<String>> dateResolve = x -> timeToSeconds(
				"(?<hour>\\d\\d):(?<min>\\d\\d):(?<sec>[0-9]+):(?<fracsec>[0-9])",
				x.key);
		modifyColumn(dir, "acc.csv", "a.csv", 'A', dateResolve, separator);
		modifyColumn(dir, "gyr.csv", "g.csv", 'A', dateResolve, separator);
		modifyColumn(dir, "mag.csv", "mag1.csv", 'A', dateResolve, separator);
		modifyColumn(dir, "mag1.csv", "m.csv", 'E', x -> Optional.empty(),
				separator);
		new File(dir, "mag1.csv").delete();
		mergeByIndex(dir, new String[] { "m.csv", "g.csv", "a.csv" },
				"combined0.csv", separator, 1);
		new File(dir, "a.csv").delete();
		new File(dir, "g.csv").delete();
		new File(dir, "m.csv").delete();
		trimPartiallyEmpty(dir, "combined0.csv", "C-readable.csv", ",");
		new File(dir, "combined0.csv").delete();
		class TimeElapser implements
				Function<Pair<String, String>, Optional<String>> {
			double start = -1;
			@Override
			public Optional<String> apply(Pair<String, String> t) {
				if (!t.key.matches("[\\-\\.0-9]+"))
					return Optional.of("Clock Time (s)" + t.value
							+ "Elapsed Time (s)");
				if (start < 0) start = Double.parseDouble(t.key);
				return Optional.of(t.key + t.value
						+ (Double.parseDouble(t.key) - start));
			}
		}
		toSpherical(dir, "C-readable.csv", "wpolar.csv", 'B', separator);
		modifyColumn(dir, "wpolar.csv", "human-readable.csv", 'A',
				new TimeElapser(), separator);
		new File(dir, "wpolar.csv").delete();
		pullFirst(dir, "human-readable.csv",
				String.format("human-readable-first-%d.csv", rows), rows);
	}
	public static Optional<String> timeToSeconds(String format, String cell) {
		Matcher mat = Pattern.compile(format).matcher(cell);
		if (!mat.find()) {
			try {
				// try just decimal data. Round to nearest tenth of a
				// second.
				return Optional.of(String.format("%.1f",
						Double.parseDouble(cell)));
			} catch (NumberFormatException e) {
				// Assume text
				return Optional.of(cell);
			}
		}
		return Optional.of(Double.toString(Double.parseDouble(mat
				.group("hour"))
				* 3600.
				+ Double.parseDouble(mat.group("min"))
				* 60.
				+ Double.parseDouble(mat.group("sec"))
				+ (mat.group("fracsec") == null ? 0 : Double
						.parseDouble(mat.group("fracsec")) * .1)));
	}
	public static void toSpherical(String dir, String in, String out,
			char colx, String sep) throws FileNotFoundException, IOException {
		final int colxi = colx - 'A';
		Function<String, String> toSpherical = line -> {
			String[] cells = line.split(sep);
			StringBuffer buff = new StringBuffer();
			for (int i = 0; i < colxi; i++) {
				buff.append(cells[i]).append(sep);
			}
			if (cells[colxi].matches("[-0-9\\.]+")) {
				double x = Double.parseDouble(cells[colxi]), y = Double
						.parseDouble(cells[colxi + 1]), z = Double
						.parseDouble(cells[colxi + 2]);
				double r = Math.sqrt(x * x + y * y + z * z);
				double theta = Math.atan2(y, x);
				if (theta < 0) theta += Math.PI * 2;
				double phi = Math.atan2(Math.sqrt(x * x + y * y), z);
				buff.append(r).append(sep).append(theta).append(sep)
						.append(phi).append(sep);
			} else {
				System.out.println(line);
				buff.append(cells[colxi].substring(0,
						cells[colxi].length() - 1));
				buff.append("r").append(sep);
				buff.append(
						cells[colxi + 1].substring(0,
								cells[colxi + 1].length() - 1))
						.append("theta").append(sep);
				buff.append(
						cells[colxi + 2].substring(0,
								cells[colxi + 2].length() - 1))
						.append("phi").append(sep);
			}
			for (int i = colxi + 3; i < cells.length; i++) {
				buff.append(cells[i]).append(sep);
			}
			return buff.substring(0, buff.length());
		};
		IO.writeLines(new File(dir, out), IO.readLines(new File(dir, in))
				.stream().map(toSpherical).collect(Collectors.toList()));
	}
	public static void trimPartiallyEmpty(String dir, String in, String out,
			String sep) throws FileNotFoundException, IOException {
		ArrayList<String> lines = IO.readLines(new File(dir, in));
		System.out.println(lines);
		ArrayList<String> output = new ArrayList<String>();
		output.add(lines.get(0));
		int start = -1;
		for (int i = 1; i < lines.size(); i++) {
			if (lines.get(i).contains(sep + sep)) continue;
			start = i;
			break;
		}
		int end = -1;
		for (int i = lines.size() - 1; i >= 0; i--) {
			if (lines.get(i).contains(sep + sep)) continue;
			end = i;
			break;
		}
		if (start > -1 && end > -1) output.addAll(lines.subList(start, end));
		IO.writeLines(new File(dir, out), output);
	}
}
