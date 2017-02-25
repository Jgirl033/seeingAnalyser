package DoubanAnalyse;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import FileOperate.HDFSFileOperator;

public class UserAnalyser {

	private HDFSFileOperator hdfsFileOperator;

	public UserAnalyser() {
		this.hdfsFileOperator = new HDFSFileOperator();
	}

	public UserAnalyser(HDFSFileOperator hdfsFileOperator) {
		this.hdfsFileOperator = hdfsFileOperator;
	}

	public static class AreaMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regex = "常居: <a href=\".*?/\">(.*?)</a>";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(line);

			if (matcher.find()) {
				String term = matcher.group(1);
				System.out.println(term);
				word.set(term);
				context.write(word, one);
			}
		}
	}

	public static class WishMovieMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regexContent = "<div class=\"substatus\">想看</div>(.*?)<div class=\"clear\"></div></div>";
			Pattern patternContent = Pattern.compile(regexContent);
			Matcher matcherContent = patternContent.matcher(line);

			String regexMovie = " <img.*? alt=\"(.*?)\".*?/>";
			Pattern patternMovie = Pattern.compile(regexMovie);

			if (matcherContent.find()) {
				String content = matcherContent.group(1);
				Matcher matcherMovie = patternMovie.matcher(content);
				System.out.println(content);
				while (matcherMovie.find()) {
					String term = matcherMovie.group(1);
					System.out.println(term);
					word.set(term);
					context.write(word, one);
				}
			}
		}

	}

	public static class CollectMovieMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regexContent = "<div class=\"substatus\">看过</div>(.*?)<div class=\"clear\"></div></div>";
			Pattern patternContent = Pattern.compile(regexContent);
			Matcher matcherContent = patternContent.matcher(line);

			String regexMovie = " <img src=\".*?\" class=\".*?\" alt=\"(.*?)\".*?/>";
			Pattern patternMovie = Pattern.compile(regexMovie);

			if (matcherContent.find()) {
				String content = matcherContent.group(1);
				System.out.println(content);
				Matcher matcherMovie = patternMovie.matcher(content);
				while (matcherMovie.find()) {
					String term = matcherMovie.group(1);
					System.out.println(term);
					word.set(term);
					context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@SuppressWarnings("deprecation")
	public void areaMapReduce(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: areacount <in> <out>");
			System.exit(2);
		}

		// 地区
		System.out.println("豆瓣用户地区统计结果如下：");
		Job job = new Job(conf, "area count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(AreaMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/douban/user/";
			this.hdfsFileOperator.downloadOutputFile(path, "areacount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: areacount <in> <out>");
				System.exit(2);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void wishMovieMapReduce(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wishMovieCount <in> <out>");
			System.exit(2);
		}

		// 地区
		System.out.println("豆瓣用户想看的电影统计结果如下：");
		Job job = new Job(conf, "wishMovie count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(WishMovieMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/douban/user/";
			this.hdfsFileOperator
					.downloadOutputFile(path, "wishMovieCount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: wishMovieCount <in> <out>");
				System.exit(2);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void collectMovieMapReduce(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wishMovieCount <in> <out>");
			System.exit(2);
		}

		// 地区
		System.out.println("豆瓣用户看过的电影统计结果如下：");
		Job job = new Job(conf, "collectMovie count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(CollectMovieMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/douban/user/";
			this.hdfsFileOperator
					.downloadOutputFile(path, "collectMovieCount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: collectMovieCount <in> <out>");
				System.exit(2);
			}
		}
	}

	public static void main(String[] args) {
		System.out.println(args[1]);
		String localFilePath = "doc/server/douban/user/";
		String[] hdfsArgsPath = { "/user/hadoop/input/seeing/douban/user",
				"output" };
		HDFSFileOperator hdfsFileOperator = new HDFSFileOperator();
		// 1.删除output文件和input文件夹
		try {
			hdfsFileOperator.deleteOutputFile();
			// hdfsFileOperator.deleteInputFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 File[] dir = new File(localFilePath).listFiles();
		 for (File file : dir) {
		 System.out.println("正在上传文件：" + localFilePath + file.getName());
		 hdfsFileOperator.uploadInputFile(localFilePath + file.getName());
		 }

		UserAnalyser doubanUserAnalyser = new UserAnalyser(
				hdfsFileOperator);
		try {
//			doubanUserAnalyser.areaMapReduce(hdfsArgsPath);
//			doubanUserAnalyser.wishMovieMapReduce(hdfsArgsPath);
			doubanUserAnalyser.collectMovieMapReduce(hdfsArgsPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
