package com.myself.big.data.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * hdfs操作类
 * 注意 1.hdfs不支持对同一个文件的并发写入
 */
@Service
@Slf4j
public class HdfsService {

//    @Value("${hadoop.core.site.path:../config/core-site.xml}")
//    private String coreSitePath;
//
//    @Value("${hadoop.dfs.hdfs.site.path:../config/hdfs-site.xml}")
//    private String hdfsSitePath;

    static ThreadLocal<DistributedFileSystem> fileSystemThreadLocal = new ThreadLocal<>();

    static ThreadLocal<FSDataOutputStream> fsDataOutputStreamThreadLocal = new ThreadLocal<>();

    private DistributedFileSystem getHadoopFileSystem() throws Exception {
        Configuration conf = getHdfsConfiguration();
        fileSystemThreadLocal.set((DistributedFileSystem)FileSystem.get(conf));
        return fileSystemThreadLocal.get();
    }

    /**
     * 获取hdfs客户端 并设置缓存区大小
     * @param num 字节大小
     * @return hdfs客户端
     * @throws Exception 异常
     */
    private DistributedFileSystem getHadoopFileSystemSize(int num) throws Exception {
        Configuration conf = getHdfsConfiguration();
        conf.setInt(DFSConfigKeys.DFS_STREAM_BUFFER_SIZE_KEY, num);
        fileSystemThreadLocal.set((DistributedFileSystem)FileSystem.get(conf));
        return fileSystemThreadLocal.get();
    }


    private Configuration getHdfsConfiguration() {
        // 这里会自动扫描classpath路径下的hadoop配置文件 也可以指定配置url等
        // conf.set("fs.defaultFS", fsUrl);
        // 指定文件路径
        Configuration configuration = new Configuration();
//        configuration.addResource(new Path(coreSitePath));
//        configuration.addResource(new Path(hdfsSitePath));
        return configuration;
    }


    /**
     * 创建文件并写入
     *
     * @param hdfsPath hdfs文件路径
     * @param content  内容
     * @throws IOException 异常
     */
    public void createFileAndInsert(String hdfsPath, String content) {
        try {
            getHadoopFileSystem();
            BufferedWriter writer = null;
            FSDataOutputStream fsDataOutputStream = null;
            Path filePath = new Path(hdfsPath);
            fsDataOutputStream = fileSystemThreadLocal.get().create(filePath);
            writer = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            writer.write(content + "\n");
            fileSystemThreadLocal.get().close();
            IOUtils.closeStream(writer);
            IOUtils.closeStream(fsDataOutputStream);
        } catch (Exception e) {
            log.warn("[hdfs] create file and insert error", e);
        }
    }


    /**
     * 指定文件追加内容
     *
     * @param hdfsPath hdfs文件路径
     * @param content  内容
     * @throws IOException 异常
     */
    public void appendToFile(String hdfsPath, String content) {
        try {
            getHadoopFileSystem();
            FSDataOutputStream fsDataOutputStream = null;
            Path filePath = new Path(hdfsPath);
            if (!fileSystemThreadLocal.get().exists(filePath)) {
                log.warn("[hdfs] append to file but not exist file , create file [{}]first", hdfsPath);
                fsDataOutputStream = fileSystemThreadLocal.get().create(filePath);
            } else {
                fsDataOutputStream = fileSystemThreadLocal.get().append(filePath);
            }
            fsDataOutputStream.write((content+"\n").getBytes(StandardCharsets.UTF_8));
            IOUtils.closeStream(fsDataOutputStream);
            fileSystemThreadLocal.get().close();
        } catch (Exception e) {
            log.warn("[hdfs] append file and insert error", e);
        }
    }


    /**
     * 批量追加写入 最后关闭流 编码格式UTF-8 换行写入
     *
     * @param hdfsPath 文件
     * @param contents 内容
     */
    public void batchAppendToFileByLine(String hdfsPath, List contents) {

        try {
            getHadoopFileSystemSize(40960);
            Path filePath = new Path(hdfsPath);
            if (!fileSystemThreadLocal.get().exists(filePath)) {
                log.warn("[hdfs] batch append to file but not exist file , create first");
                createFileStream(filePath);
            } else {
                appendFileStream(filePath);
            }

            for (int i = 0; i < contents.size(); i++) {
                try {
                     fsDataOutputStreamThreadLocal.get().write((JSONObject.toJSONString(contents.get(i)) + "\n").getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    log.error("[hdfs] batch append to file error", e);
                }
            }
            IOUtils.closeStream(fsDataOutputStreamThreadLocal.get());
            fileSystemThreadLocal.get().close();

        } catch (Exception e) {
            log.error("[hdfs] batch to hdfs error", e);
        }

    }

    private void appendFileStream(Path filePath) throws IOException {
        fsDataOutputStreamThreadLocal.set(fileSystemThreadLocal.get().append(filePath));
    }

    private void createFileStream(Path filePath) throws IOException {
        fsDataOutputStreamThreadLocal.set(fileSystemThreadLocal.get().create(filePath));
    }


    /**
     * 读取文件
     *
     * @param hdfsPath hdfs文件路径
     * @return 返回结果
     * @throws IOException io异常
     */
    public byte[] readFile(String hdfsPath) throws IOException {
        try {
            getHadoopFileSystem();
            Path filePath = new Path(hdfsPath);
            if (!fileSystemThreadLocal.get().exists(filePath)) {
                throw new IOException("[hdfs] file doesn't exist: " + hdfsPath);
            }
            FSDataInputStream inputStream = fileSystemThreadLocal.get().open(filePath);
            byte[] buffer = new byte[inputStream.available()];
            inputStream.readFully(buffer);
            fileSystemThreadLocal.get().close();
            return buffer;
        } catch (Exception e) {
            log.error("[hdfs] read file error", e);
        }
        return null;
    }


    /**
     * 删除文件
     *
     * @param hdfsPath hdfs路径
     */
    public boolean deleteFile(String hdfsPath) throws Exception {
        getHadoopFileSystem();
        Path filePath = new Path(hdfsPath);
        boolean isDeleted = fileSystemThreadLocal.get().delete(filePath, false);  // 第二个参数指定是否递归删除文件夹
        fileSystemThreadLocal.get().close();
        return isDeleted;
    }


    /**
     * 罗列有哪些hdfs文件
     * 返回的文件名称有可能还是个目录
     *
     * @param hdfsPath 目录
     * @return 文件名(有可能是目录)
     * @throws Exception io异常
     */
    public List<String> getAllFiles(String hdfsPath) throws Exception {
        List<String> allFileNames = new ArrayList<>();
        getHadoopFileSystem();
        Path directoryPath = new Path(hdfsPath);
        FileStatus[] fileStatuses = fileSystemThreadLocal.get().listStatus(directoryPath);
        for (FileStatus fileStatus : fileStatuses) {
            allFileNames.add(fileStatus.getPath().getName());
        }
        fileSystemThreadLocal.get().close();
        return allFileNames;
    }


    /**
     * 通过流的方式将原文件上传到hdfs
     *
     * @param orgFilePath    原文件地址
     * @param targetFilePath hdfs目标文件地址
     * @throws Exception 异常
     */
    public void putFileToHdfsBuStream(String orgFilePath, String targetFilePath) throws Exception {
        // 1 获取文件系统
        getHadoopFileSystem();
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File(orgFilePath));
        // 3 获取输出流
        FSDataOutputStream fos = fileSystemThreadLocal.get().create(new Path(targetFilePath));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, getHdfsConfiguration());
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystemThreadLocal.get().close();
    }


    /**
     * 从hdfs上下载文件
     *
     * @throws IOException          异常
     * @throws InterruptedException 异常
     * @throws URISyntaxException   异常
     */
    public void getFileFromHdfs(String orgFilePath, String targetFilePath) throws Exception {
        // 1 获取文件系统
        getHadoopFileSystem();
        // 2 获取输入流
        FSDataInputStream fis = fileSystemThreadLocal.get().open(new Path(targetFilePath));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(orgFilePath));
        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, getHdfsConfiguration());
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystemThreadLocal.get().close();
    }


    public boolean existPath(Path filePath) {
        try {
            getHadoopFileSystem();
            if (fileSystemThreadLocal.get().exists(filePath)) {
                return true;
            }
        } catch (Exception e) {
            log.error("[hdfs] existPath error", e);
        }
        return false;
    }
}
