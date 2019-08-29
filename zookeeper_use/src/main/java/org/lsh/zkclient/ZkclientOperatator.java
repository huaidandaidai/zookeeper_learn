package org.lsh.zkclient;

import org.I0Itec.zkclient.ZkClient;

import java.util.List;

/**
 * ZKclient使用
 *  需要引用zkclient maven配置
 *  <dependency>
 *         <groupId>com.101tec</groupId>
 *         <artifactId>zkclient</artifactId>
 *         <version>0.10</version>
 *     </dependency>
 */
public class ZkclientOperatator {
    static final String CONNECT_ADDR = "127.0.0.1";
    static final int SESSION_OUTTIME = 1000;

    public static void main(String[] args) throws InterruptedException {
        ZkClient zkc= new ZkClient(CONNECT_ADDR,SESSION_OUTTIME);
        //1. create and delete方法
        zkc.createEphemeral("/temp");
        zkc.createPersistent("/super/c1", true);

        Thread.sleep(10000);
        zkc.delete("/temp");
        zkc.deleteRecursive("/super");


        //2. 设置path和data 并且读取子节点和每个节点的内容
        zkc.createPersistent("/super", "1234");
        zkc.createPersistent("/super/c1", "c1内容");
        zkc.createPersistent("/super/c2", "c2内容");


        List<String> list = zkc.getChildren("/super");
        for(String p : list){
            System.out.println(p);
            String rp = "/super/" + p;
            String data = zkc.readData(rp);
            System.out.println("节点为：" + rp + "，内容为: " + data);
        }

        //3. 更新和判断节点是否存在
        zkc.writeData("/super/c1", "新内容");
        System.out.println(zkc.readData("/super/c1").toString());
        System.out.println(zkc.exists("/super/c1"));

//		4.递归删除/super内容
        zkc.deleteRecursive("/super");

    }
}
