package cn.zwz.article.vote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.ZParams.Aggregate;
import cn.zwz.util.PoolManager;

/**
 * <p>
 * 第一次写折腾jedis，包括连接处理都还没想出太好的方案
 * 1、仿照redis in action 写的;
 * 2、包括主题发布，主题投票，主题取值等
 * <p>
 * @author tianxingjian
 *
 */
public class ArticleVote {
	
	private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;
    
    private static String ARTICLE_BEF = "article:";
    private static String VOTED_BEF = "voted:";
    private static String SCORE_BEF= "score:";
    private static String TIME_BEF = "time:";
    private static String GROUP_BEF = "group:";
    
	
    /**
     * <p>
     * 发布主题：
     * 1、往主题投票信息set中插入一条数据
     * 2、主题Map中插入主题信息
     * 3、票数统计信息ZSET中加入当前主题的票数，票数按时间折合成了一票等于432秒（为了按票数统计主题）
     * 4、主题发布时间ZSET中加入当前主题的发布时间（为了按时间统计主题）
     * <p>
     * 
     * @author tianxingjian
     * @date 2014-10-22
     * 
     * @param userName
     * @param title
     * @param url
     * 
     * @return 返回主题Id
     */
	public String postArticle(String userName,  String title, String url){
		
		Jedis jedis = getPoolManager().getJedis();
		
		String articleId = String.valueOf(jedis.incr(ARTICLE_BEF));
	
		 String voted = VOTED_BEF + articleId;
		jedis.sadd(voted, userName);
		jedis.expire(voted, ONE_WEEK_IN_SECONDS);
		
		long now = System.currentTimeMillis() / 1000;
		String article = ARTICLE_BEF + articleId;
		Map<String, String> map = new HashMap<String, String>();
		map.put("title", title);
		map.put("link", url);
		map.put("user",userName);
		map.put("time", String.valueOf(now));
		map.put("votes", "1");
		
		jedis.hmset(article, map);
		
		jedis.zadd(SCORE_BEF, now + VOTE_SCORE,  article);
		jedis.zadd(TIME_BEF,  now , article);
		
		realeseJedis(jedis);
		
		return articleId;
	}
	
	/**
	 * <p>
	 * 投票：超过一个星期不允许投票， 每个相同投票没只有一张票生效
	 * 
	 * <p>
	 * @param userName
	 * @param article
	 * @return
	 */
    public boolean addVote(String userName, String article){
    	
    	long cutOff = System.currentTimeMillis() /1000 - ONE_WEEK_IN_SECONDS;
    	Jedis conn = getPoolManager().getJedis();
    	
    	//超过一星期不让投票
    	if(conn.zscore(TIME_BEF, article)  < cutOff){
    		return false;
    	}
    	
    	 String articleId = article.substring(article.indexOf(':') + 1);
    	if(conn.sadd(VOTED_BEF, userName) == 1){
    		 conn.zincrby(SCORE_BEF, VOTE_SCORE, articleId);
    		 conn.hincrBy(article, "votes", 1L);
    		 realeseJedis(conn);
    	}else {
    		return false;
    	}
    	
    	return true;
    }
	
    /**
     * <p>
     * 分页取出artile
     *  写到这个方法才发现取连接有那么点问题，前头代码不修改了这个方法起修改
     * <p>
     * @param page
     * @param order 写这个方法前好使的分页是根据发布时间为权值排序和
     * 投票权值排序分页，目前就这两个吧，所以order暂时就是score: ,time:
     * @return
     */
    public List<Map<String, String>> getArticles(int page, String order){
    	
    	int start = (page -1 ) * ARTICLES_PER_PAGE ;
    	int end = page*ARTICLES_PER_PAGE - 1;
    	
    	Jedis conn = null;
    	List<Map<String, String>> resultList = new ArrayList<Map<String, String>>() ;
    	try{
    		conn = getPoolManager().getJedis();
    		Set<String> ids = conn.zrevrange(order, start, end);
    		for(String id : ids){
    			 Map<String, String> map = conn.hgetAll(id);
    			 if(map != null && !map.isEmpty()){
    				 resultList.add(map);
    			 }
    		}
    	}finally{
    		 realeseJedis(conn);
    	}
    	return resultList;
    }
    
    /**
     * <p>
     * 将一组主题加入到多个分组中
     * <p>
     * @param groups
     * @param articles
     */
    public void addGrop(String []groups, String []articles){
    	Jedis conn = null;
    	try{
    		conn = getPoolManager().getJedis();

    		for(String group : groups){
    			conn.sadd(GROUP_BEF + group, articles);
    		}
    	}finally{
    		 realeseJedis(conn);
    	}
    }
    
    /**
     *  <p>
     *  有分组就有分组成员，此方法是取得分组成员
     *  <p>
     * @param group 分组名称
     * @return
     */
    public List<Map<String, String>> getGroupMembers(String group){
    	Jedis conn = null;
    	List<Map<String, String>>  resultList  = new ArrayList<Map<String, String>>();
    	try{
    		conn = getPoolManager().getJedis();
    		Set<String> ids = conn.smembers(GROUP_BEF + group);
    		for(String id : ids){
   			 	Map<String, String> map = conn.hgetAll(id);
   			 	if(map != null && !map.isEmpty()){
   			 		resultList.add(map);
   			 	}
    		}
    	}finally{
    		 realeseJedis(conn);
    	}
    	return resultList;
    }
    
    /**
     * <p>
     * 主题组按排名分页取值：其实就是通过group和order做连接，生成类似关系型数据库的临时表（虽然非关系型数据库使用
     * 用关系型数据库来理解不太合理， 但确实就这个意思）
     * <p>
     * @param group 组名
     * @param order 写这个方法前好使的分页是根据发布时间为权值排序和
     * 投票权值排序分页，目前就这两个吧，所以order暂时就是score: ,time:
     * @param page 第n页
     * @return
     */
    public List<Map<String, String>> getGroupPageMembers(String group, String order, int page){
    	
    	Jedis conn = null;
    	List<Map<String, String>>  resultList  = new ArrayList<Map<String, String>>();
    	try{
    		String newkey = group + order + System.currentTimeMillis(); //临时表就做得特殊点，最大程度避免key已经被使用
    		conn = getPoolManager().getJedis();
    		
    		if(!conn.exists(newkey)){
    			ZParams params = new ZParams().aggregate(Aggregate.MAX); 
    			conn.zinterstore(newkey, params, group, order); //按照最大值排序生成合集
    			conn.expire(newkey, 60); //设置key的生存时间，也就是说60秒就消失
    			resultList = getArticles(page, newkey);
    			//conn.del(newkey); //想法是要删除临时表，但是得测试在这种方法是否可行， 有conn.expire这么好使的这个貌似就没存在意义了
    		}
    	}finally{
    		 realeseJedis(conn);
    	}
    	return resultList;
    }
    
	public Map<String, String> getArticleById(String articleId){
		
		Jedis jedis = getPoolManager().getJedis();
		Map<String, String> map = jedis.hgetAll(ARTICLE_BEF + articleId); 
		realeseJedis(jedis);
		
		return map;
	}
	/**
	 * <p>
	 * 批量打印Article
	 * <p>
	 * @param articles
	 */
    public void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  title: " + article.get("title"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("title")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
    

    /**
	 * <p>
	 * 单独打印Article
	 * <p>
	 * @param articles
	 */
    public void printArticle(Map<String,String>article){
    	
		System.out.println("  title: " + article.get("title"));
		for (Map.Entry<String, String> entry : article.entrySet()) {
			if (entry.getKey().equals("title")) {
				continue;
			}
			System.out.println("    " + entry.getKey() + ": "+ entry.getValue());
		}
		
    }
	
	private void realeseJedis( Jedis redis){
		
		try{
			getPoolManager().returnPoolResource(redis);
		}finally{
			redis = null;
		}
		
	}
	private PoolManager getPoolManager(){
		return PoolManager.getInstance();
	}
}
