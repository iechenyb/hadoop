package com.cyb.zookeeper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
/**
 * zkServer.cmd启动
 * zkCli.cmd访问目录服务
 * ls /  查看目录
 * create /zk myData 创建目录
 * @author DHUser
 *http://blog.csdn.net/LK10207160511/article/details/50530212
 *version 3.4.6
 */
public class ZooClientTest {
	private static final Log log = LogFactory.getLog(ZooClientTest.class);
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ZookeeperClient client = new ZookeeperClient();
		String host = "192.168.16.211:2181";
		// 连接zookeeper
		client.connectZookeeper(host);
		log.info("zookeeper连接成功！");

		// 创建节点
		//String result = client.createNode("/test1", "我是该节点的数据内容！".getBytes());
		//log.info(result + "节点创建成功！");
		//client.setData("/test1", "xxxxxxx".getBytes(), 1);
		

		// 获取某路径下所有节点
		List<String> children = client.getChildren("/");
		for (String child : children) {
			log.info("一级目录："+child);
		}
		log.info("成功获取child节点");

		// 获取节点数据
		byte[] nodeData = client.getData("/test1");
		log.info("获取节点数据："+new String(nodeData));

		/*// 更新节点数据
		data = "test data".getBytes();
		client.setData("/test", data, 0);
		log.info("成功更新节点数据！");
		nodeData = client.getData("/test");*/
		client.closeConnect();
	}
}
