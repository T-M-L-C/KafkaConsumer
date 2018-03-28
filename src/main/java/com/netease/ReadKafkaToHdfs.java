package com.netease;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;

/**
 * @author liuchao
 * @since 2/27/2018
 * 为了保证HDFS服务的高可用,生产环境是必须要开启NameNode HA的,此时应该用nameservices作为统一的logical name连接HDFS.
 */
public class ReadKafkaToHdfs {

    private static Configuration conf=new Configuration(false);

    public static final String KEYTAB_FILE_KEY = "hdfs.keytab.file";

    public static final String USER_NAME_KEY = "hdfs.kerberos.principal";

    static {
        String nameservices = "hz-cluster3";
        String[] namenodesAddr = {"hadoop278.lt.163.org:8020","hadoop279.lt.163.org:8020"};
        String[] namenodes = {"nn1","nn2"};
        conf.set("fs.defaultFS", "hdfs://" + nameservices);
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("dfs.nameservices",nameservices);
        conf.set("dfs.ha.namenodes." + nameservices, namenodes[0]+","+namenodes[1]);
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
        conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
        conf.set("dfs.client.failover.proxy.provider." + nameservices,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@HADOOP.HZ.NETEASE.COM");
        conf.addResource(new Path("/home/appops/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/appops/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/home/appops/hadoop/etc/hadoop/yarn-site.xml"));
        //使用klist命令查看kerberos中凭证标示和keytab文件路径。
        //keytab文件的路径
        conf.set(KEYTAB_FILE_KEY, "/home/appops/hadoop/etc/hadoop/algo.keytab");
        //principal
        conf.set(USER_NAME_KEY, "algo/dev@HADOOP.HZ.NETEASE.COM");
    }
    public static void writeToHdfs(String ad_log) throws Exception{
        JSONObject jsonObject = JSONObject.parseObject(ad_log);
        String dateTime = jsonObject.get("@timestamp").toString();
        String date = dateTime.split("T")[0];
        String hour = dateTime.split("T")[1].split(":")[0];
        String filePath = "rankserver/"+date+"-"+hour+".log";

        UserGroupInformation.setConfiguration(conf);
        login(conf);
        Path path = new Path(filePath);
        FileSystem hdfs = FileSystem.get(conf);
        if(!hdfs.exists(path)){
            hdfs.createNewFile(path);
            hdfs.close();
        }
        InputStream in = new BufferedInputStream(new ByteArrayInputStream(ad_log.getBytes()));
        OutputStream out = hdfs.append(path);
        int byteread = 0;
        // 一次读多个字节
        byte[] tempbytes = new byte[1024];
        while ((byteread = in.read(tempbytes)) != -1) {
            out.write(tempbytes,0,byteread);
        }
        out.write("\n".getBytes());
        out.close();
        hdfs.close();
        System.out.println("时间戳为"+dateTime+"对应的展点日志成功写入"+" : "+"rankserver/"+date+"-"+hour+".log"+"中");
    }

    public static void login(Configuration hdfsConfig) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            String keytab = conf.get(KEYTAB_FILE_KEY);
            if (keytab != null) {
                hdfsConfig.set(KEYTAB_FILE_KEY, keytab);
            }
            String userName = conf.get(USER_NAME_KEY);
            if (userName != null) {
                hdfsConfig.set(USER_NAME_KEY, userName);
            }
            SecurityUtil.login(hdfsConfig, KEYTAB_FILE_KEY, USER_NAME_KEY);
        }
    }
}
