package cn.zwz.component;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import cn.zwz.util.PoolDao;

/**
 * <p>
 * 简单的自动匹配工具
 * <p>
 * @author tianxingjian
 *
 */
public class RecentContact extends PoolDao{
	
	/**
	 * <p>
	 * 添加用户当前搜索的词
	 * <p>
	 * @param user
	 * @param contact
	 */
    public void addUpdateContact( String user, String contact) {
    	Jedis conn = null;
    	try{
    		conn = this.getPoolManager().getJedis();
    		Pipeline pipe = conn.pipelined();
    		
    		String acList = "recent:" + user;
    		
    		pipe.multi();
    		pipe.lrem(acList, 0, contact);
    		pipe.lpush(acList, contact);
    		pipe.ltrim(acList, 0, 99);
    		pipe.exec();
    	}finally{
    		this.realeseJedis(conn);
    	}
        
    }

    /**
     * 删除用户搜索的词
     * @param user
     * @param contact
     */
	public void removeContact(String user, String contact) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			conn.lrem("recent:" + user, 0, contact);
		} finally {
			this.realeseJedis(conn);
		}
	}

	/**
	 * 按前缀匹配历史带选词
	 * @param user
	 * @param prefix
	 * @return
	 */
    public List<String> fetchAutocompleteList(String user, String prefix) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			List<String> candidates = conn.lrange("recent:" + user, 0, -1);
			List<String> matches = new ArrayList<String>();
			for (String candidate : candidates) {
				if (candidate.toLowerCase().startsWith(prefix)) {
					matches.add(candidate);
				}
			}
			return matches;
		} finally {
			this.realeseJedis(conn);
		}
    }
}
