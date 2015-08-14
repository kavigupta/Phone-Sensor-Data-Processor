package dataprocessor.data.manipulation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import dataprocessor.io.IO;
import dataprocessor.struct.Pair;

public class SpreadsheetHandler {
	/**
	 * Assumes that all columns have headers
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void mergeByIndex(String dir, String[] in, String out,
			String separator, int limit) throws FileNotFoundException,
			IOException {
		String header = null;
		TreeMap<Double, ArrayList<ArrayList<String>>> values = new TreeMap<>();
		int[] cols = new int[in.length];
		for (int i = 0; i < in.length; i++) {
			ArrayList<String> lines = IO.readLines(new File(dir, in[i]));
			String[] cells = lines.get(0).split(separator);
			cols[i] = cells.length;
			if (header == null) header = cells[0] + separator;
			for (int j = 1; j < cells.length; j++) {
				header += IO.nameOf(in[i]) + cells[j] + separator;
			}
			for (int row = 1; row < lines.size(); row++) {
				try {
					int sep = lines.get(row).indexOf(separator);
					double key = Double.parseDouble(lines.get(row)
							.substring(0, sep));
					if (values.get(key) == null) {
						ArrayList<ArrayList<String>> table = new ArrayList<ArrayList<String>>();
						for (int i2 = 0; i2 < in.length; i2++) {
							table.add(new ArrayList<>());
						}
						values.put(key, table);
					}
					if (values.get(key).get(i).size() < limit)
						values.get(key)
								.get(i)
								.add(lines.get(row).substring(
										sep + separator.length()));
				} catch (RuntimeException e) {}
			}
		}
		String[] empty = new String[in.length];
		for (int i = 0; i < in.length; i++) {
			empty[i] = pad("", separator, cols[i]);
		}
		ArrayList<String> result = new ArrayList<String>();
		result.add(header);
		for (Entry<Double, ArrayList<ArrayList<String>>> e : values
				.entrySet()) {
			int max = 0;
			for (int i = 0; i < e.getValue().size(); i++) {
				max = Math.max(max, e.getValue().get(i).size());
			}
			for (int i = 0; i < e.getValue().size(); i++) {
				for (int j = e.getValue().get(i).size(); j < max; j++) {
					e.getValue().get(i).add(empty[i]);
				}
			}
			for (int rowN = 0; rowN < e.getValue().get(0).size(); rowN++) {
				String row = e.getKey() + separator;
				for (int i = 0; i < e.getValue().size(); i++) {
					row += e.getValue().get(i).get(rowN);
				}
				result.add(row);
			}
		}
		IO.writeLines(new File(dir, out), result);
	}
	/**
	 * The function is (cell, separator)-> Optional(new cell)
	 */
	public static void modifyColumn(String dir, String in, String out,
			char columnName,
			Function<Pair<String, String>, Optional<String>> modifier,
			String separator) throws FileNotFoundException, IOException {
		int column = columnName - 'A';
		ArrayList<String> input = IO.readLines(new File(dir, in));
		ArrayList<String> output = new ArrayList<>(input.size());
		for (String ln : input) {
			String[] csv = ln.split(separator);
			StringBuffer newLine = new StringBuffer();
			int woffset = 0;
			for (int i = 0; i < csv.length; i++) {
				if (i == column + woffset) {
					Optional<String> cell = modifier.apply(Pair
							.getInstance(csv[i], separator));
					if (cell.isPresent())
						newLine.append(cell.get()).append(separator);
					else woffset++;
				} else newLine.append(csv[i]).append(separator);
			}
			output.add(newLine.toString());
		}
		IO.writeLines(new File(dir, out), output);
	}
	public static void merge(String dir, String[] in, String out,
			String separator) throws FileNotFoundException, IOException {
		@SuppressWarnings("unchecked")
		List<String>[] lines = new List[in.length];
		int[] columns = new int[in.length];
		int maxRows = 0;
		for (int i = 0; i < lines.length; i++) {
			lines[i] = IO.readLines(new File(dir, in[i]));
			OptionalInt col = lines[i].stream()
					.mapToInt(x -> x.split(separator).length).max();
			int nColumn = col.isPresent() ? col.getAsInt() : 0;
			columns[i] = nColumn;
			maxRows = Integer.max(maxRows, lines[i].size());
			lines[i] = lines[i].stream()
					.map(x -> pad(x, separator, nColumn))
					.collect(Collectors.toList());
		}
		for (int i = 0; i < lines.length; i++) {
			pad(lines[i], separator, columns[i], maxRows);
		}
		ArrayList<String> combination = new ArrayList<>();
		for (int i = 0; i < maxRows; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j < lines.length; j++) {
				sb.append(lines[j].get(i));
			}
			combination.add(sb.toString());
		}
		IO.writeLines(new File(dir, out), combination);
	}
	private static void pad(List<String> list, String separator, int columns,
			int rows) {
		String components = pad("", separator, columns);
		for (int i = list.size(); i <= rows; i++)
			list.add(components);
	}
	private static String pad(String line, String separator, int to) {
		int from = line.split(separator).length;
		for (int i = from; i < to; i++)
			line += separator;
		return line;
	}
	public static Optional<String> dateTimeSplitter(
			Pair<String, String> cellsep) {
		Matcher mat = Pattern.compile(
				"(\\d\\d\\d\\d-\\d\\d-\\d\\d) (\\d\\d?:\\d\\d)").matcher(
				cellsep.key);
		if (!mat.find()) return Optional.empty();
		return Optional.of(mat.group(1) + cellsep.value + mat.group(2));
	}
	public static void pullFirst(String dir, String in, String out, int rows)
			throws FileNotFoundException, IOException {
		IO.writeLines(new File(dir, out),
				IO.readLines(new File(dir, in), rows));
	}
}
