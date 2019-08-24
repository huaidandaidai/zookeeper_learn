package org.lsh.zkjavaapi;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class TestJavaAPI implements Watcher {
    private static final int SESSION_TIMEOUT = 10000;
    private static final String CONNECTION_STRING = "127.0.0.1:2181";
    private static final String ZK_PATH = "/node";

    private ZooKeeper zk =null;

    private CountDownLatch cdl = new CountDownLatch(1);

    public static void main(String[] args) {
        TestJavaAPI testJavaAPI= new TestJavaAPI();
        testJavaAPI.createConnection(CONNECTION_STRING,SESSION_TIMEOUT);
        System.out.println("===========");
        if(testJavaAPI.createPath(ZK_PATH,"初始节点内容")){
            System.out.println("数据内容："+testJavaAPI.readData(ZK_PATH));
            testJavaAPI.writeData(ZK_PATH,"content after update");
            System.out.println("更新后的内容"+testJavaAPI.readData(ZK_PATH));
            testJavaAPI.deletePath(ZK_PATH);
        }
    }

    /**
     * 创建zk链接
     * @param connectString
     * @param sessionTimeout
     */
    public void createConnection(String connectString, int sessionTimeout){
        try {
            zk = new ZooKeeper(connectString,sessionTimeout,this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *  关闭zk链接
     */
    public void releaseConnection(){
        if(null !=zk){
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建节点
     * @param path 节点path
     * @param data 初始数据内容
     * @return
     */
    public boolean createPath(String path,String data){
        try {
            System.out.println("节点创建成功,Path:"+
                    this.zk.create(path,//节点路径
                            data.getBytes(),//节点数据
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,//节点权限
                            CreateMode.EPHEMERAL)+//节点类型
                    ", content;"+ data);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 读取指定节点数据
     * @param path
     * @return
     */
    public String readData(String path){
        System.out.println("获取数据成功："+path);
        try {
            return  new String(this.zk.getData(path,false,null));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 向指定节点写入数据
     * @param path
     * @param data
     * @return
     */
    public boolean writeData(String path, String data){
        try {
            System.out.println("更新数据成功，path："+path+",state:"+this.zk.setData(path,data.getBytes(),-1));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除指定节点
     * @param path
     */
    public void deletePath(String path){
        try {
            this.zk.delete(path,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("收到事件通知："+watchedEvent.getState());
        if (Event.KeeperState.SyncConnected == watchedEvent.getState()){
            cdl.countDown();
        }
    }
}
