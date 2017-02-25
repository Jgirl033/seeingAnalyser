package DoubanAnalyse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

public class UserMovieAnalyser {

	private HDFSFileOperator hdfsFileOperator;

	public UserMovieAnalyser() {
		this.hdfsFileOperator = new HDFSFileOperator();
	}

	public UserMovieAnalyser(HDFSFileOperator hdfsFileOperator) {
		this.hdfsFileOperator = hdfsFileOperator;
	}

	public static class UserMovieStyleMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String regex = "<span class=\"pl\">类型:</span>(.*?)</span>";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(line);

			if (matcher.find()) {
				String term = matcher.group(1).replaceAll("<.*?>", "");
				String[] style = term.split("/");
				System.out.println(term);
				for (String s : style) {
					word.set(s);
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
	public void userMovieStyleMapReduce(String[] args, String mid)
			throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: stylecount <in> <out>");
			System.exit(2);
		}

		// 地区
		System.out.println("豆瓣用户偏好电影类型统计结果如下：");
		Job job = new Job(conf, "style count");
		job.setJarByClass(UserAnalyser.class);
		job.setMapperClass(UserMovieStyleMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(false) == true) {
			String path = "doc/analyser/douban/movie/";
			this.hdfsFileOperator.downloadOutputFile(path, mid
					+ "stylecount.txt");
			this.hdfsFileOperator.deleteOutputFile();

			Configuration conf1 = new Configuration();
			String[] otherArgs1 = new GenericOptionsParser(conf1, args)
					.getRemainingArgs();
			if (otherArgs1.length != 2) {
				System.err.println("Usage: stylecount <in> <out>");
				System.exit(2);
			}
		}
	}

	public static ArrayList<String> getUserMovie(String commentSourceCode) {

		ArrayList<String> uidList = new ArrayList<>();
		ArrayList<String> userMovieList = new ArrayList<>();
		Pattern pattern = Pattern
				.compile("<div class=\"comment-item\" data-cid=\".*?\">.*?<div class=\"avatar\">.*?<a title=\"(.*?)\" href=\"https://www.douban.com/people/(.*?)/\">.*?<img src=\"(.*?)\".*?>.*?</a>.*?</div>.*?<div class=\"comment\">.*?<span class=\"votes pr5\">(.*?)</span>.*?<span class=\"allstar(.*?) rating\" title=\".*?\"></span>.*?<span class=\"comment-time \">(.*?)</span>.*?<p class=\"\">(.*?)</p>.*?</div>");
		Matcher matcher = pattern.matcher(commentSourceCode);

		while (matcher.find()) {
			String uid = matcher.group(2);
			uidList.add(uid);
			Reader r = new Reader("doc/server/douban/user/", uid + ".txt");

			String content = "";
			int count = 1;
			Pattern pattern1 = Pattern
					.compile("<div class=\"substatus\">看过</div>(.*?)<div class=\"clear\"></div></div>");
			Matcher matcher1 = pattern1.matcher(r.read());
			if (matcher1.find()) {
				content = matcher1.group(1).trim();
			}

			Pattern pattern2 = Pattern
					.compile("<a href=\"https://movie.douban.com/subject/(.*?)/\" title=\"(.*?)\" target=\"_blank\">");
			Matcher matcher2 = pattern2.matcher(content);
			while (matcher2.find()) {
				switch (count) {
				case 1: {
					userMovieList.add(matcher2.group(1).trim());
					break;
				}
				case 2: {
					userMovieList.add(matcher2.group(1).trim());
					break;
				}
				case 3: {
					userMovieList.add(matcher2.group(1).trim());
					break;
				}
				case 4: {
					userMovieList.add(matcher2.group(1).trim());
					break;
				}
				case 5: {
					userMovieList.add(matcher2.group(1).trim());
					break;
				}
				}
				count++;
			}

		}
		return userMovieList;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String[] midList = {};
		for (String mid : midList) {
			String commentPath = "doc/server/douban/comment/";
			Reader r = new Reader(commentPath, mid + ".txt");
			ArrayList<String> userMovieList = getUserMovie(r.read());

			String localFilePath = "doc/server/douban/movie/";
			String[] hdfsArgsPath = { "/user/hadoop/input/seeing/douban/movie",
					"output" };
			HDFSFileOperator hdfsFileOperator = new HDFSFileOperator(
					"/user/hadoop/input/seeing/douban/movie",
					"/user/hadoop/output");

			try {
				hdfsFileOperator.deleteOutputFile();
				hdfsFileOperator.deleteInputFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// 1.上传观看过该电影的用户想看/看过的电影文件
			ArrayList<File> dir = new ArrayList<>();
			for (String userMovie : userMovieList) {
				dir.add(new File(localFilePath + userMovie + ".txt"));
			}
			for (File file : dir) {
				System.out.println("正在上传文件：" + localFilePath + file.getName());
				hdfsFileOperator
						.uploadInputFile(localFilePath + file.getName());
			}

			UserMovieAnalyser doubanUserAnalyser = new UserMovieAnalyser(
					hdfsFileOperator);
			try {
				doubanUserAnalyser.userMovieStyleMapReduce(hdfsArgsPath, mid);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

}
