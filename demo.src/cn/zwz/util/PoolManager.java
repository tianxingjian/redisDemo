package cn.zwz.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PoolManager {

	private static PoolManager instance = null;

	private static final Lock instancelock = new ReentrantLock();
	
	private final Lock opLock = new ReentrantLock();

	private JedisPool pool = null;

	private PoolManager() {
		
		JedisPoolConfig config = new JedisPoolConfig();
		// 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
		// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
		config.setMaxActive(500);
		// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
		config.setMaxIdle(5);
		// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
		config.setMaxWait(1000 * 100);
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		config.setTestOnBorrow(true);

		pool = new JedisPool(config, HostAndPortConfig.getDefaultHostPort()
				.getHost(), HostAndPortConfig.getDefaultHostPort()
				.getPort());
	}

	/**
	 * <p>
	 * 取实例， 加锁判断实例是否为空，为空则进行初始化。
	 * <p>
	 * 
	 * @author tianxingjian
	 * @return
	 */
	public static PoolManager getInstance() {

		instancelock.lock();
		try {
			if (instance == null) {
				instance = new PoolManager();
			}
		} finally {
			instancelock.unlock();
		}

		return instance;
	}

	/**
	 * <p>
	 * 取redis
	 * <p>
	 * 
	 * @author tianxingjian
	 * @return
	 */
	public Jedis  getJedis() {
		return pool.getResource();
	}

	public void returnPoolResource(Jedis redis) {
		if (pool != null) {
			pool.returnResource(redis);
		}
	}

}
