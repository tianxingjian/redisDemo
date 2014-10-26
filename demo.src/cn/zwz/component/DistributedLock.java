package cn.zwz.component;

import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import cn.zwz.util.PoolDao;

/**
 * <p>
 *	通過Redis實現分佈式鎖
 * <p>
 * @author tianxingjian
 *
 */
public class DistributedLock extends PoolDao {

	public String aquireLock(String lockName) {
		return aquireLock(lockName, 10000);
	}

	/**
	 * <p>
	 * 申请加锁， 当超过aquireTimeOut加锁不成功则返回null退出
	 * 缺點：當持有當前對象鎖得線程因爲某種原因沒釋放鎖時，其他線程就一直得不到鎖
	 * <p>
	 * 
	 * @param lockName 鎖對象
	 * @param aquireTimeOut 超時時間
	 * @return
	 */
	private String aquireLock(String lockName, int aquireTimeOut) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String lockIndentify = UUID.randomUUID().toString();
			Long end = System.currentTimeMillis() + aquireTimeOut;
			while (System.currentTimeMillis() < end) {
				if (conn.setnx("lock:" + lockName, lockIndentify) == 1) {
					return lockIndentify;
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupted();
				}
			}
			return null;
		} finally {
			this.realeseJedis(conn);
		}
	}
	
	/**
	 * <p>
	 * 帶lockTimeout的鎖申請：
	 * 線程得到鎖的時間超過lockTimeout後會被其他申請相同對象的鎖的線程踢出
	 * 是對aquireLock列出的缺點的彌補， 當超過一個時間後線程還沒釋放鎖時就強制釋放。
	 * 缺點：當持有鎖的線程還在操作就被強制釋放時，多線程沒法保證線程安全
	 * <p>
	 * @param lockName
	 * @param acquireTimeout
	 * @param lockTimeout
	 * @return
	 */
	public String aquireLockWithTimeOut(String lockName, long acquireTimeout, long lockTimeout){
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String lockIndentify = UUID.randomUUID().toString();
			
			String lockKey = "lock:" + lockName;
	        int lockExpire = (int)(lockTimeout / 1000);
	        
			Long end = System.currentTimeMillis() + acquireTimeout;
			while (System.currentTimeMillis() < end) {
				if (conn.setnx(lockKey, lockIndentify) == 1) {
					conn.expire(lockKey, lockExpire);
					return lockIndentify;
				}else if(conn.ttl(lockKey) == -1){
					 conn.exists(lockKey);
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupted();
				}
			}
			return null;
		} finally {
			this.realeseJedis(conn);
		}
	}

	/**
	 * <p>
	 * 釋放鎖
	 * <p>
	 * @param lockName
	 * @param identifier
	 * @return
	 */
	public boolean realeaseLock(String lockName, String identifier){
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String lockKey = "lock:" + lockName;
			  while (true){
		            conn.watch(lockKey);
		            if (identifier.equals(conn.get(lockKey))){//判斷當前鎖標識符是否等於線程要釋放的鎖標識符
		                Transaction trans = conn.multi();
		                trans.del(lockKey);
		                List<Object> results = trans.exec();
		                if (results == null){
		                    continue;
		                }
		                return true;
		            }
		            conn.unwatch();
		            break;
		        }
		        return false;

		} finally {
			this.realeseJedis(conn);
		}
	}
}
