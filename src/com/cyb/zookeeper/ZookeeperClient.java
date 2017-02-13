	package com.cyb.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

	public class ZookeeperClient implements Watcher
	{
		private static final Log log = LogFactory.getLog(ZookeeperClient.class);
		public ZooKeeper zookeeper;
		private static int SESSION_TIME_OUT = 2000;
		private CountDownLatch countDownLatch = new CountDownLatch(1);
		
		/**
		 * 连接zookeeper
		 * @param host
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void connectZookeeper(String host) throws IOException, InterruptedException{
			zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
			countDownLatch.await();
			log.info("zookeeper connect ok");
		}
		
		/**
		 * 实现watcher的接口方法，当连接zookeeper成功后，zookeeper会通过此方法通知watcher
		 * 此处为如果接受到连接成功的event，则countDown，让当前线程继续其他事情。
		 */
		public void process(WatchedEvent event) {
			if(event.getState() == KeeperState.SyncConnected){
				log.info("watcher receiver event");
				countDownLatch.countDown();
			}
		}
	    
		/**
		 * 根据路径创建节点，并且设置节点数据
		 * @param path
		 * @param data
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		public String createNode(String path,byte[] data) throws KeeperException, InterruptedException{
			return this.zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		/**
		 * 根据路径获取所有孩子节点 
		 * @param path
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		public List<String> getChildren(String path) throws KeeperException, InterruptedException{
			return this.zookeeper.getChildren(path, false);
		}
		
		public Stat setData(String path,byte[] data,int version) throws KeeperException, InterruptedException{
			return this.zookeeper.setData(path, data, version);
		}
		
		/**
		 * 根据路径获取节点数据
		 * @param path
		 * @return
		 * @throws KeeperException
		 * @throws InterruptedException
		 */
		public byte[] getData(String path) throws KeeperException, InterruptedException{
			return this.zookeeper.getData(path, false, null);
		}
		
		/**
		 * 删除节点
		 * @param path
		 * @param version
		 * @throws InterruptedException
		 * @throws KeeperException
		 */
		public void deleteNode(String path,int version) throws InterruptedException, KeeperException{
			this.zookeeper.delete(path, version);
		}
		
		/**
		 * 关闭zookeeper连接
		 * @throws InterruptedException
		 */
		public void closeConnect() throws InterruptedException{
			if(null != zookeeper){
				zookeeper.close();
			}
		}
		 
	}