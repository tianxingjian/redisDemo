package cn.zwz.component;

import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;
import cn.zwz.util.PoolDao;

/**
 * <p>
 * Redis實現分佈式信號量
 * <p>
 * @author tianxingjian
 *
 */
public class RedisSemaphore extends PoolDao {

	/**
	 * <p>
	 * 申请信号量： 信号量时有容量限制的，当目前分配的信号量超过容量时拒绝分配。使用ZSET的权值将先后申请的信号量按时间权值排序
	 * 当信号量申请时间超时后自动清除。如果分配成功则返回信号量值，否则返回空
	 * <p>
	 * 
	 * @param semname
	 * @param timeOut
	 * @param limit
	 * @return
	 */
	public String acquireSemaphore(String semname, long timeOut, long limit) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String indentifier = UUID.randomUUID().toString();
			Pipeline pipe = conn.pipelined();
			long now = System.currentTimeMillis();

			pipe.zremrangeByScore(semname, Double.MIN_VALUE, now - timeOut);
			pipe.zadd(semname, now, indentifier);
			pipe.zrank(semname, indentifier);
			List<Object> list = pipe.syncAndReturnAll();

			if ((Long) list.get(list.size() - 1) < limit) {// 排名在limit范围内说明有可分配的信号量
				return indentifier;
			}

			pipe.zrem(semname, indentifier);
			return null;
		} finally {
			this.realeseJedis(conn);
		}
	}

	/**
	 * 信号量释放
	 * 
	 * @param semName
	 * @param indentifier
	 */
	public void realeaseSemaphore(String semName, String indentifier) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			conn.zrem(semName, indentifier);
		} finally {
			this.realeseJedis(conn);
		}
	}

	/**
	 * <p>
	 * 相對比較公平的分佈式信號量申請：
	 * 排序的時候用counter代替客戶端時間作爲權值，解決多臺機器時間有誤差造成的排序不準，從而導致某些機器
	 * 容易申請到信號量，某些機器難申請到信號量
	 * <p>
	 * @param semname
	 * @param timeOut
	 * @param limit
	 * @return
	 */
	public String acquireFairSemaphore(String semname, long timeOut, long limit) {
		
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String ownerKey = semname + ":owner";
			String instr = semname + ":counter";

			String indentifier = UUID.randomUUID().toString();
			
			long now = System.currentTimeMillis();
			Transaction trans = conn.multi();
			trans.zremrangeByScore(semname.getBytes(), "-inf".getBytes(), String
					.valueOf(now - timeOut).getBytes());//也不是完全公平的，有可能某个信号量会先删
			ZParams params = new ZParams();
			params.weights(1, 0); // 用于ZinterStore求和的时候取权值
			trans.zinterstore(ownerKey, params, ownerKey, semname);// 最后两个参数的先后关系不能错，否则权值取错计算就错了
			trans.incr(instr);
			List<Object> list = trans.exec();
			int counter = ((Long) list.get(list.size() - 1)).intValue();
			
			//用pipe操作会死锁，返回的为QUEUED，这个问题待解决
			// Pipeline pipe = conn.pipelined();
			// pipe.multi();
			// pipe.zadd(semname, now, indentifier);
			// pipe.zadd(ownerKey, counter, indentifier);
			// pipe.zrank(ownerKey, indentifier);
			// pipe.syncAndReturnAll();

			trans = conn.multi();
			trans.zadd(semname, now, indentifier);
			trans.zadd(ownerKey, counter, indentifier);
			trans.zrank(ownerKey, indentifier);
			list = trans.exec();
			
			int result = ((Long) list.get(list.size() - 1)).intValue();
			if (result < limit) {
				return indentifier;
			}
			
			//只有沒取到信號量纔會走到這
			trans = conn.multi();
			trans.zrem(semname, indentifier);
			trans.zrem(ownerKey, indentifier);
			trans.exec();
			return null;
			
		} finally {
			this.realeseJedis(conn);
		}
	}
	
	public void releaseFairSemaphore(String semname, String indentifier){
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String ownerKey = semname + ":owner";
			
			Pipeline pipe = conn.pipelined();
			pipe.zrem(semname, indentifier);
			pipe.zrem(ownerKey, indentifier);
			pipe.sync();
			
		} finally {
			this.realeseJedis(conn);
		}
	}
}
