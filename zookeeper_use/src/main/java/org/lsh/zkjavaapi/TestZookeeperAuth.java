package org.lsh.zkjavaapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestZookeeperAuth implements Watcher {
    //连接地址
    private static final String CONNECTION_ADDRESS="127.0.0.1:2181";
    //测试路径
    private static final String PATH="/auth";
    private static final String PATH_DEL="/auth/delNode";
    //认证类型
    private static final String authentition_type="digest";
    //认证正确方值
    private static final String correctAuthentication="123456";
    //认证错误值
    private final static String badAuthentication = "654321";

    private static ZooKeeper zk=null;
    //计数器
    AtomicInteger seq= new AtomicInteger();

    //标识
    private static final String LOG_PREFIX_OF_MAIN = "【main】";

    private CountDownLatch cdl=new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        TestZookeeperAuth testZookeeperAuth= new TestZookeeperAuth();
        testZookeeperAuth.createConnection(CONNECTION_ADDRESS,2000);

        List<ACL> acls = new ArrayList<ACL>(1);
        for (ACL acl : ZooDefs.Ids.CREATOR_ALL_ACL) {
            acls.add(acl);
        }
        try {
            zk.create(PATH,"Init content".getBytes(),acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key：" + correctAuthentication + "创建节点："+ PATH + ", 初始内容是: init content");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            zk.create(PATH_DEL, "will be deleted! ".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key：" + correctAuthentication + "创建节点："+ PATH_DEL + ", 初始内容是: init content");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 获取数据
        getDataByNoAuthentication();
        getDataByBadAuthentication();
        getDataByCorrectAuthentication();

        // 更新数据
        updateDataByNoAuthentication();
        updateDataByBadAuthentication();
        updateDataByCorrectAuthentication();

        // 删除数据
        deleteNodeByBadAuthentication();
        deleteNodeByNoAuthentication();
        deleteNodeByCorrectAuthentication();
        //
        Thread.sleep(1000);

        deleteParent();
        //释放连接
        testZookeeperAuth.releaseConnection();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(null == watchedEvent){
            return;
        }
        //连接状态
        Event.KeeperState state= watchedEvent.getState();
        //事件类型
        Event.EventType type= watchedEvent.getType();
        //受影响的path
        String path= watchedEvent.getPath();

        String logPrefix = "【Watcher-"+this.seq.incrementAndGet()+"】";
        System.out.println(logPrefix+"收到Watcher通知");
        System.out.println(logPrefix+"连接状态：\t" +state.toString());
        System.out.println(logPrefix+"事件类型：\t"+ type.toString());
        if(Event.KeeperState.SyncConnected == state){
            //成功连上服务器
            if (Event.EventType.None == type) {
                System.out.println(logPrefix+"成功连接上了zk服务器");
                cdl.countDown();
            }
        }else if(Event.KeeperState.Disconnected ==state){
            System.out.println(logPrefix + "与ZK服务器断开连接");
        }else if(Event.KeeperState.AuthFailed == state){
            System.out.println(logPrefix + "权限检查失败");
        }else if(Event.KeeperState.Expired == state){
            System.out.println(logPrefix + "会话失效");
        }
        System.out.println("------------------------------------------");
    }

    /**
     * 创建连接
     * @param address
     * @param sessionTimeout
     */
    public void createConnection(String address,int sessionTimeout){
        this.releaseConnection();
        try {
            this.zk = new ZooKeeper(address,sessionTimeout,this::process);
            //添加节点授权
            zk.addAuthInfo(authentition_type,correctAuthentication.getBytes());
            System.out.println(LOG_PREFIX_OF_MAIN+"开始连接zk服务器");
            //倒数等待
            cdl.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放连接
     */
    public void releaseConnection(){
        if(null == zk){
            return;
        }
        try {
            this.zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /** 获取数据：采用错误的密码 */
    static void getDataByBadAuthentication() {
        String prefix = "[使用错误的授权信息]";
        try {
            ZooKeeper badzk = new ZooKeeper(CONNECTION_ADDRESS, 2000, null);
            //授权
            badzk.addAuthInfo(authentition_type,badAuthentication.getBytes());
            Thread.sleep(2000);
            System.out.println(prefix + "获取数据：" + PATH);
            System.out.println(prefix + "成功获取数据：" + badzk.getData(PATH, false, null));
        } catch (Exception e) {
            System.err.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    /** 获取数据：不采用密码 */
    static void getDataByNoAuthentication() {
        String prefix = "[不使用任何授权信息]";
        try {
            System.out.println(prefix + "获取数据：" + PATH);
            ZooKeeper nozk = new ZooKeeper(CONNECTION_ADDRESS, 2000, null);
            Thread.sleep(2000);
            System.out.println(prefix + "成功获取数据：" + nozk.getData(PATH, false, null));
        } catch (Exception e) {
            System.err.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    /** 采用正确的密码 */
    static void getDataByCorrectAuthentication() {
        String prefix = "[使用正确的授权信息]";
        try {
            System.out.println(prefix + "获取数据：" + PATH);

            System.out.println(prefix + "成功获取数据：" + zk.getData(PATH, false, null));
        } catch (Exception e) {
            System.out.println(prefix + "获取数据失败，原因：" + e.getMessage());
        }
    }

    /**
     * 更新数据：不采用密码
     */
    static void updateDataByNoAuthentication() {

        String prefix = "[不使用任何授权信息]";

        System.out.println(prefix + "更新数据： " + PATH);
        try {
            ZooKeeper nozk = new ZooKeeper(CONNECTION_ADDRESS, 2000, null);
            Thread.sleep(2000);
            Stat stat = nozk.exists(PATH, false);
            if (stat!=null) {
                nozk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix + "更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "更新失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 更新数据：采用错误的密码
     */
    static void updateDataByBadAuthentication() {

        String prefix = "[使用错误的授权信息]";

        System.out.println(prefix + "更新数据：" + PATH);
        try {
            ZooKeeper badzk = new ZooKeeper(CONNECTION_ADDRESS, 2000, null);
            //授权
            badzk.addAuthInfo(authentition_type,badAuthentication.getBytes());
            Thread.sleep(2000);
            Stat stat = badzk.exists(PATH, false);
            if (stat!=null) {
                badzk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix + "更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "更新失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 更新数据：采用正确的密码
     */
    static void updateDataByCorrectAuthentication() {

        String prefix = "[使用正确的授权信息]";

        System.out.println(prefix + "更新数据：" + PATH);
        try {
            Stat stat = zk.exists(PATH, false);
            if (stat!=null) {
                zk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix + "更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "更新失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 不使用密码 删除节点
     */
    static void deleteNodeByNoAuthentication() throws Exception {

        String prefix = "[不使用任何授权信息]";

        try {
            System.out.println(prefix + "删除节点：" + PATH_DEL);
            ZooKeeper nozk = new ZooKeeper(CONNECTION_ADDRESS, 2000, null);
            Thread.sleep(2000);
            Stat stat = nozk.exists(PATH_DEL, false);
            if (stat!=null) {
                nozk.delete(PATH_DEL,-1);
                System.out.println(prefix + "删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "删除失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 采用错误的密码删除节点
     */
    static void deleteNodeByBadAuthentication() throws Exception {

        String prefix = "[使用错误的授权信息]";

        try {
            System.out.println(prefix + "删除节点：" + PATH_DEL);
            ZooKeeper badzk = new ZooKeeper(CONNECTION_ADDRESS, 2000, null);
            //授权
            badzk.addAuthInfo(authentition_type,badAuthentication.getBytes());
            Thread.sleep(2000);
            Stat stat = badzk.exists(PATH_DEL, false);
            if (stat!=null) {
                badzk.delete(PATH_DEL, -1);
                System.out.println(prefix + "删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix + "删除失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 使用正确的密码删除节点
     */
    static void deleteNodeByCorrectAuthentication() throws Exception {

        String prefix = "[使用正确的授权信息]";

        try {
            System.out.println(prefix + "删除节点：" + PATH_DEL);
            Stat stat = zk.exists(PATH_DEL, false);
            if (stat!=null) {
                zk.delete(PATH_DEL, -1);
                System.out.println(prefix + "删除成功");
            }
        } catch (Exception e) {
            System.out.println(prefix + "删除失败，原因是：" + e.getMessage());
        }
    }

    /**
     * 使用正确的密码删除节点
     */
    static void deleteParent() throws Exception {
        try {
            Stat stat = zk.exists(PATH_DEL, false);
            if (stat == null) {
                zk.delete(PATH, -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
