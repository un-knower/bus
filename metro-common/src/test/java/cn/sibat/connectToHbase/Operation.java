package cn.sibat.connectToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

import java.io.*;
import java.util.ArrayList;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static cn.sibat.connectToHbase.Util.contentLimit;

/**
 * 读取文件列表到队列，执行多线程读取文件内容到队列
 * Created by wing1995 on 2017/8/7.
 */

public class Operation {

    long start = System.currentTimeMillis() / 1000;

    private static Configuration hbaseConfig = null;
    private static HTablePool pool = null;
    private static String tableName = "GdRoadTest";

    private static int count = 0;
    private static LinkedBlockingDeque<File> files;//即将上传的文件队列
    private static ExecutorService UploadThreadPool = Executors.newFixedThreadPool(5);//上传文件线程池

    static{
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.40.49");
        hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);

        pool = new HTablePool(hbaseConfig, 1000);
    }

    /**
     * 构造器，初始化文件队列
     * 	@param path 需要读取文件的文件夹路径
     */
    public Operation(String path) {

        files = new LinkedBlockingDeque<>();

        ReadFiles(path);

    }

    /**
     * 获取文件列表并将文件添加到files队列
     * @param path 文件夹路径
     */
    private void ReadFiles(String path){

        System.out.println(path);
        File file=new File(path);

        for(int i=0;i<file.listFiles().length;i++){
            files.add(file.listFiles()[i]);
        }

    }

    /**
     * 将文件从队列中获取，并返回到Util工具类进行多线程读文件操作
     */
    public void MultiReadFiles(){

        while(files.size()>0){
            try {

                new Util(files.take());

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public void insertToHbase(String content) throws IOException {

        HTableInterface table = pool.getTable(tableName);
        table.setAutoFlushTo(false);
        table.setWriteBufferSize(10 * 1024 * 1024);

        ArrayList<Put> list = new ArrayList<>();

        String[] lineList = content.split("\n");

        for(String line: lineList) {
            //若提交的列表数目等于指定行，则提交列表；否则往列表里面添加行
            if (list.size() == contentLimit) {

                table.put(list);
                table.flushCommits();
                list.clear();
                System.out.println("已插入数据" + count + "条");

            }

            String arr_value[] = line.split(",", 2);

            String rowKey = arr_value[0];
            String value = arr_value[1];

            Put p = new Put(rowKey.getBytes());

            p.add(("value").getBytes(), "".getBytes(), value.getBytes());
            list.add(p);

            count++;
        }

        //将最后的几行数据添加到表
        if (list.size() > 0) {

            table.put(list);
            table.flushCommits();

        }

        table.close();
    }

    /**
     * 多线程处理数据
     */
    public void MultiDeal(){

        UploadThreadPool.execute(() -> {

            //若文件内容为空，则等待一秒继续读取文件
            while (Util.Contents.size() == 0) {
                try {

                    System.out.println("等待一秒");
                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            //若文件内容队列不为空则对文件内容执行操作
            while (Util.Contents.size() > 0) {

                System.out.println("now numbers:" + Util.Contents.size());
                String content = null;

                try {

                    content = Util.Contents.take();
                    //对文件内容进行插入到表操作
                    insertToHbase(content);

                } catch (Exception e) {
                    try {

                        e.printStackTrace();
                        //处理失败，内容重回队列，准备再次处理数据
                        Util.Contents.put(content);

                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }
            }

            System.out.println("now numbers:" + Util.Contents.size());

            try {
                Thread.sleep(1000 * 2);//线程终止2秒
                //若内容为空，且文件队列为空则停止程序
                if (Util.Contents.size() == 0 && files.size() == 0) {
                    long end = System.currentTimeMillis() / 1000;
                    System.out.println("多线程多客户端插入数据耗时" + (end - start) + "秒");
                    System.exit(0);
                }

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }
}