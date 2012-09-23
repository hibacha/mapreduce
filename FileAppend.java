package hadoop.demo;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;

public class FileAppend2 {

	public static final String uriBackUp = "hdfs://localhost:9000/user/zhouyaofei/backup.tmp";
	public static final String uriTargetPrefix = "hdfs://localhost:9000/user/zhouyaofei/";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Options customizedOptions = buildOption();

		if (args.length == 0) {
			printUsage(customizedOptions);
			return;
		}
		try {
			GenericOptionsParser parser = new GenericOptionsParser(conf,
					customizedOptions, args);
			CommandLine cmdLine = parser.getCommandLine();
			if (cmdLine.hasOption("t") && cmdLine.hasOption("c")) {

				String targetPath = cmdLine.getOptionValue("t");
				String content = cmdLine.getOptionValue("c");

				processCommand(targetPath, content, conf);

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block

		} catch (NullPointerException e) {

		}
	}

	public static void backup(Configuration conf, FileSystem fs,
			FSDataInputStream sourceContent) throws IOException {

		FSDataOutputStream out = fs.create(new Path(uriBackUp), true);
		IOUtils.copyBytes(sourceContent, out, 4096, false);
		out.close();

	}

	private static void processCommand(String targetPath, String content,
			Configuration conf) throws Exception {
		Path path = new Path(uriTargetPrefix + targetPath);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);

		backup(conf, fs, in);

		in.close();

		FSDataOutputStream out = fs.create((path), true);
		FSDataInputStream backup = fs.open(new Path(uriBackUp));

		int offset = 0;
		int bufferSize = 4096;

		int result = 0;

		byte[] buffer = new byte[bufferSize];
		// pre read a part of content from input stream
		result = backup.read(offset, buffer, 0, bufferSize);
		// loop read input stream until it does not fill whole size of buffer
		while (result == bufferSize) {
			out.write(buffer);
			// read next segment from input stream by moving the offset pointer
			offset += bufferSize;
			result = backup.read(offset, buffer, 0, bufferSize);
		}

		if (result > 0 && result < bufferSize) {
			for (int i = 0; i < result; i++) {
				out.write(buffer[i]);
			}

		}
		out.writeBytes(content + "\n");
		out.close();

	}

	public static void printUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter writer = new PrintWriter(System.out);
		formatter.printHelp(writer, 80, "FileAppend [generic option] ",
				"Begin", options, 4, 5, "end", true);
		writer.close();
	}

	@SuppressWarnings("static-access")
	public static Options buildOption() {
		Options options = new Options();

		Option t = OptionBuilder
				.hasArg()
				.withArgName("TargetName")
				.withDescription("hdfs file path you want to append content to")
				.isRequired().withLongOpt("target").create("t");

		Option c = OptionBuilder
				.hasArg()
				.withArgName("Content")
				.withDescription(
						"content you want to append to the target file")
				.isRequired().withLongOpt("content").create("c");

		options.addOption(t);
		options.addOption(c);
		return options;
	}
}
