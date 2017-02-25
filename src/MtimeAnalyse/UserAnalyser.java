package MtimeAnalyse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
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
import FileOperate.Reader;

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
			String regex = "我的时光.*?&lt;p class=\\\\\"mt9\\\\\"&gt;.*?，(.*?)&lt;/p&gt;.*?&lt;p class=\\\\\"mt6\\\\\"&gt;个人博客";
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

	public static class SexMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regex = "我的时光.*?&lt;p class=\\\\\"mt9\\\\\"&gt;(.*?)，.*?&lt;/p&gt;.*?&lt;p class=\\\\\"mt6\\\\\"&gt;个人博客";
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

	public static class AgeMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String term;
			Calendar a = Calendar.getInstance();
			int year = a.get(Calendar.YEAR);
			int age = 0;

			Pattern pattern1 = Pattern
					.compile("生日：([0-9]{4})年([0-9]{1,2})月([0-9]{1,2})日");
			Matcher matcher1 = pattern1.matcher(line);

			Pattern pattern2 = Pattern.compile("生日：([0-9]{4})年([0-9]{1,2})月");
			Matcher matcher2 = pattern2.matcher(line);

			if (matcher1.find()) {
				age = year - Integer.parseInt(matcher1.group(1));
			} else if (matcher2.find()) {
				age = year - Integer.parseInt(matcher2.group(1));
			} else {
				age = -1;
			}

			if (age == -1) {
				term = "0";
			} else if (age < 12) {
				term = "1";
			} else if (age < 18) {
				term = "2";
			} else if (age < 22) {
				term = "3";
			} else if (age < 30) {
				term = "4";
			} else {
				term = "5";
			}
			word.set(term);
			context.write(word, one);
		}
	}

	public static class EducationMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regex = "教育信息(.*?)职业信息";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(line);

			Pattern patternItem = Pattern
					.compile("&lt;p class=\\\\\"mt6\\\\\"&gt;.*?&lt;a href=.*?title=\\\\\"(.*?)\\\\\"&gt;");
			if (matcher.find()) {
				String term = matcher.group(1);
				System.out.println(term);
				Matcher matcherItem = patternItem.matcher(term);
				while (matcherItem.find()) {
					System.out.println(matcherItem.group(1));
					word.set(matcherItem.group(1));
					context.write(word, one);
				}
			}
		}
	}

	public static class JobMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regex = "职业信息(.*?)关注";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(line);

			Pattern patternItem = Pattern
					.compile("&lt;p class=\\\\\"mt6\\\\\"&gt;.*?&lt;a href=.*?searchtype=4\\\\\"&gt;(.*?)&lt;");
			if (matcher.find()) {
				String term = matcher.group(1);
				System.out.println(term);
				Matcher matcherItem = patternItem.matcher(term);
				while (matcherItem.find()) {
					System.out.println(matcherItem.group(1));
					word.set(matcherItem.group(1));
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
	public void areaMapReduce(String[] args, String mid) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: areacount <in> <out>");
			System.exit(2);
		}

		// 地区
		System.out.println("时光网用户地区统计结果如下：");
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
			String path = "doc/analyser/mtime/user/";
			this.hdfsFileOperator.downloadOutputFile(path, mid
					+ "areacount.txt");
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
	public void sexMapReduce(String[] args, String mid) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: areacount <in> <out>");
			System.exit(2);
		}

		// 地区
		System.out.println("时光网用户性别统计结果如下：");
		Job job = new Job(conf, "sex count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(SexMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/mtime/user/";
			this.hdfsFileOperator
					.downloadOutputFile(path, mid + "sexcount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: sexcount <in> <out>");
				System.exit(2);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void ageMapReduce(String[] args, String mid) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: agecount <in> <out>");
			System.exit(2);
		}

		System.out.println("时光网用户年龄统计结果如下：");
		Job job = new Job(conf, "age count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(AgeMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/mtime/user/";
			this.hdfsFileOperator
					.downloadOutputFile(path, mid + "agecount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: agecount <in> <out>");
				System.exit(2);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void educationMapReduce(String[] args, String mid) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: educationcount <in> <out>");
			System.exit(2);
		}

		System.out.println("时光网用户教育背景统计结果如下：");
		Job job = new Job(conf, "education count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(EducationMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/mtime/user/";
			this.hdfsFileOperator.downloadOutputFile(path, mid
					+ "educationcount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: educationcount <in> <out>");
				System.exit(2);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void jobMapReduce(String[] args, String mid) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: jobcount <in> <out>");
			System.exit(2);
		}

		System.out.println("时光网用户职业背景统计结果如下：");
		Job job = new Job(conf, "education count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(JobMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/mtime/user/";
			this.hdfsFileOperator
					.downloadOutputFile(path, mid + "jobcount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: jobcount <in> <out>");
				System.exit(2);
			}
		}
	}

	public static ArrayList<String> getUid(String commentSourceCode) {

		ArrayList<String> uidList = new ArrayList<>();
		Pattern pattern = Pattern
				.compile("<h3>(.*?)</h3>.*?<a target=\"_blank\" title=\"(.*?)\" href=\"http://my.mtime.com/(.*?)/\">.*?</a>.*?<p class=\"px14\">.*?<p class=\"mt6 px12 clearfix\">(.*?)<div class=\"mt10\"><a href=\".*?\" target=\"_blank\" entertime=\"(.*?)\"></a></div>");
		Matcher matcher = pattern.matcher(commentSourceCode);
		while (matcher.find()) {
			uidList.add(matcher.group(3).trim());
		}
		return uidList;
	}

	public static void main(String[] args) {
		String[] midList = { "123883", "153307", "207372", "214815", "232556" };
		for (String mid : midList) {
			
			String hotCommentPath = "doc/server/mtime/comment/hot/";
			String newCommentPath = "doc/server/mtime/comment/new/";
			Reader rc = new Reader(hotCommentPath, mid + ".txt");
			Reader rn = new Reader(newCommentPath, mid + ".txt");

			ArrayList<String> uidList = getUid(rc.read());
			uidList.addAll(getUid(rn.read()));

			String localFilePath = "doc/server/mtime/user/profile/";
			String[] hdfsArgsPath = { "/user/hadoop/input/seeing/mtime/user",
					"output" };
			HDFSFileOperator hdfsFileOperator = new HDFSFileOperator(
					"/user/hadoop/input/seeing/mtime/user",
					"/user/hadoop/output");

			try {
				hdfsFileOperator.deleteOutputFile();
				hdfsFileOperator.deleteInputFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (String uid : uidList) {
				System.out.println("正在上传文件：" + localFilePath + uid + ".txt");
				hdfsFileOperator.uploadInputFile(localFilePath + uid + ".txt");
			}

			UserAnalyser mtimeUserAnalyser = new UserAnalyser(hdfsFileOperator);
			try {
//				mtimeUserAnalyser.areaMapReduce(hdfsArgsPath, mid);
//				mtimeUserAnalyser.sexMapReduce(hdfsArgsPath, mid);
//				mtimeUserAnalyser.ageMapReduce(hdfsArgsPath, mid);
				mtimeUserAnalyser.educationMapReduce(hdfsArgsPath, mid);
//				mtimeUserAnalyser.jobMapReduce(hdfsArgsPath, mid);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
