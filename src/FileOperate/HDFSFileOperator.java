package FileOperate;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static Constant.HDFSArgument.hdfsPath;

public class HDFSFileOperator {

	private String hdfsInputPath;
	private String hdfsOutputPath;

	public HDFSFileOperator() {
		this.hdfsInputPath = "/user/hadoop/input/seeing/douban/user";
		this.hdfsOutputPath = "/user/hadoop/output";
	}

	public HDFSFileOperator(String hdfsInputPath, String hdfsOutputPath) {
		this.hdfsInputPath = hdfsInputPath;
		this.hdfsOutputPath = hdfsOutputPath;
	}

	public void uploadInputFile(String locaFilelPath) {

		try {
			Configuration conf = new Configuration();
			FileSystem file = FileSystem.get(conf);
			file.mkdirs(new Path(this.hdfsInputPath));

			FileSystem fs = FileSystem.get(URI.create(hdfsPath + "/"), conf);
			fs.copyFromLocalFile(new Path(locaFilelPath), new Path(hdfsPath
					+ "/" + this.hdfsInputPath));
			fs.close();
			System.out.println("已经上传到input文件夹啦");
			System.out.println("hdfs文件路径是" + hdfsPath.replace("/", "")
					+ this.hdfsInputPath);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void downloadOutputFile(String locaFilelPath, String fileName)
			throws IOException {
		String remoteFile = hdfsPath + this.hdfsOutputPath + "/"
				+ "part-r-00000";
		File file = new File(remoteFile);
		if (file.exists()) {
			file.renameTo(new File(locaFilelPath + "/", fileName));
		}

		Path path = new Path(remoteFile);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfsPath + "/"), conf);
		fs.copyToLocalFile(path, new Path(locaFilelPath + "/", fileName));//注意Path的参数
		System.out.println("已经将文件保留到本地文件");
		fs.close();
	}

	public void deleteInputFile() throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(hdfsPath + "/" + this.hdfsInputPath);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath + "/"), conf);
		fs.deleteOnExit(path);
		fs.close();
		System.out.println("input文件已删除");
	}

	public void deleteOutputFile() throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(hdfsPath + "/" + hdfsOutputPath);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath + "/"), conf);
		fs.deleteOnExit(path);
		fs.close();
		System.out.println("output文件已删除");
	}
}
