package cn.zwz.cookies;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import cn.zwz.util.PoolManager;

public class LoginCookie {
	
	private static String LOGIN_REF = "login:";
	private static String RECENT_REF = "recent:";
	private static String VIEW_REF = "view:";
	private static Integer SIZE_INDEX = -26;
	private static Integer CLEAR_YU = 1000; // session 阈值，到达这个数才删cookie
	private static Integer MAX_DEL = 100;

	/**
	 *  <P>
	 *  更新cookie，保存cookie，cookie对应的当期登录人，同一cookie最新的25个
	 *  访问栏目
	 *  <P>
	 * @param token
	 * @param user
	 * @param item
	 */
	public void updateCookie(String token, String user, String item) {
		Jedis conn = null;
		try {
			conn = getPoolManager().getJedis();
			Long time = System.currentTimeMillis();
			conn.hset(LOGIN_REF, token, user);
			conn.zadd(RECENT_REF, time, token);
			if(item != null && item.trim().length() > 0){
				conn.zadd(VIEW_REF + token, time , item);
				conn.zremrangeByRank(VIEW_REF + token, 0, SIZE_INDEX);
			}

		} finally {
			realeseJedis(conn);
		}

	}
	
	/**
	 * <p>
	 * 查看cookie token 对应的用户
	 * <p>
	 * @param token
	 * @return
	 */
	public String checkCookie(String token){
		Jedis conn = null;
		String result = null;
		try {
			conn = getPoolManager().getJedis();
			result = conn.hget(LOGIN_REF, token);

		} finally {
			realeseJedis(conn);
		}
		return result;
	}
	
	/**
	 * <p>
	 * 释放cookie：
	 * 1、当且仅当cookie数目到达一个阈值时候才释放部分cookie
	 * 2、每次释放cookie得数目有一个最大数值MAX_DEL
	 * 3、释放一个cookie就是删除login记录，view记录，最近活跃记录
	 * ps：做后台自动释放可以通过线程调用此方法， Redis in Action书写的是在这个方法体里面循环调用
	 * 个人认为这个会有一个连接被一直占用， 所以舍弃while循环，可以把while循环放到线程类中
	 * <p>
	 * @throws InterruptedException
	 */
	public void clearSession() throws InterruptedException{
		
		Jedis conn = null;
		
		try {
			conn = getPoolManager().getJedis();
			Long size = conn.zcard(LOGIN_REF);
			if(size < CLEAR_YU){
				Thread.sleep(1000);
				return;
			}

			Long indexSize = (size - CLEAR_YU)  < MAX_DEL ? (size - CLEAR_YU) : MAX_DEL;
			
			Set<String> tokens = conn.zrange(RECENT_REF, 0, indexSize);
			List<String> views = new ArrayList<String>();
			for(String str : tokens){
				views.add(str);
			}
			conn.hdel(LOGIN_REF, tokens.toArray(new String[0]));
			conn.zrem(RECENT_REF, tokens.toArray(new String[0]));
			conn.del(views.toArray(new String[0]));
		
		} finally {
			realeseJedis(conn);
		}
		
	}
	

	private void realeseJedis(Jedis redis) {

		try {
			getPoolManager().returnPoolResource(redis);
		} finally {
			redis = null;
		}
	}

	private PoolManager getPoolManager() {
		return PoolManager.getInstance();
	}
}
