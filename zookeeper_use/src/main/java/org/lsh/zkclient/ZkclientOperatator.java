package org.lsh.zkclient;

import org.I0Itec.zkclient.ZkClient;

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

    public static void main(String[] args) {
        ZkClient zkc= new ZkClient(CONNECT_ADDR,SESSION_OUTTIME);


    }
}
