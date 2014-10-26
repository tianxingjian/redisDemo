package cn.zwz.component;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import cn.zwz.util.PoolDao;

/**
 * <p>
 * 前缀匹配，具体业务见Redis In Action 第六章
 * 根据ZSET特性进行处理
 * <p>
 * @author tianxingjian
 *
 */
public class AddressBookAutoCom  extends PoolDao{
	
	private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
	/**
	 * <p>
	 * 根据前缀计算一个跟前缀匹配的一个区间
	 * 如前缀是abc 算出来得区间是[abb{, abc{],在zset中按字典排序后就可以查出这两两个字符串的位置，通过Zrank就可以取出所有
	 * 的匹配值
	 * <p>
	 * @param prefix
	 * @return
	 */
    public String[] findPrefixRange(String prefix) {
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + '{';
        String end = prefix + '{';
        return new String[]{start, end};
    }
    
    /**
     * 将user加入到战队中
     * @param guild
     * @param user
     */
    public void joinGuild(String guild, String user) {
    	Jedis conn = null;
    	try{
    		conn = this.getPoolManager().getJedis();
            conn.zadd("members:" + guild, 0, user);
    	}finally{
    		this.realeseJedis(conn);
    	}
    }
    
    public void leaveGuild(String guild, String user) {
     	Jedis conn = null;
    	try{
    		conn = this.getPoolManager().getJedis();
    		conn.zrem("members:" + guild, user);
    	}finally{
    		this.realeseJedis(conn);
    	}
    }
    
    /**
     * <p>
     * 前缀过滤，返回匹配的10个字符串:
     * 1、求出区间可以匹配的区间
     * 2、将区间的两个字符串加入到Zset
     * 3、ZRANK 取出候选值
     * <p>
     * @param guild
     * @param prefix
     * @return
     */
    public Set<String> fetchAutocompleteList(String guild, String prefix){
    	Jedis conn = null;
      	try{
    		conn = this.getPoolManager().getJedis();
    		String[] prefixrange = this.findPrefixRange(prefix);
    		
    		String start = prefixrange[0];
    		String end = prefixrange[1];
    		String uuid = UUID.randomUUID().toString();
    		start = start + uuid;
    		end = end + uuid;
    	     String zsetName = "members:" + guild;

			conn.zadd(zsetName, 0, start);
			conn.zadd(zsetName, 0, end);
			
			Set<String> items = null;
	        while (true){
	            conn.watch(zsetName);
	            int sindex = conn.zrank(zsetName, start).intValue(); //zset 的特性，当权值一样时按member值字典排序
	            int eindex = conn.zrank(zsetName, end).intValue();
	            int erange = Math.min(sindex + 9, eindex - 2);//这是确定只返回10个值

	            Transaction trans = conn.multi();
	            trans.zrem(zsetName, start);
	            trans.zrem(zsetName, end);
	            trans.zrange(zsetName, sindex, erange);
	            List<Object> results = trans.exec();
	            if (results != null){
	                items = (Set<String>)results.get(results.size() - 1);
	                break;
	            }
	        }

	        for (Iterator<String> iterator = items.iterator(); iterator.hasNext(); ){
	            if (iterator.next().indexOf('{') != -1){
	                iterator.remove();
	            }
	        }
	        return items;
    	}finally{
    		this.realeseJedis(conn);
    	}
    }
}
