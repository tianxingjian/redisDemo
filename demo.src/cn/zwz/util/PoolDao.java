package cn.zwz.util;

import redis.clients.jedis.Jedis;

public class PoolDao {
	
	public void realeseJedis(Jedis redis) {
		try {
			getPoolManager().returnPoolResource(redis);
		} finally {
			redis = null;
		}
	}

	public PoolManager getPoolManager() {
		return PoolManager.getInstance();
	}
}
