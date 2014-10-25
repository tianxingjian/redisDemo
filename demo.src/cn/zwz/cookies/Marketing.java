package cn.zwz.cookies;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import cn.zwz.util.PoolDao;

/**
 * <p>
 * 根据 Redis in Action 第四章写的，主要是掌握事务着一块
 * <p>
 * @author tianxingjian
 *
 */
public class Marketing extends PoolDao {

	public static String USER_REF = "user:";
	public static String INVENTORY_REF = "inventory:";
	public static String MARKET_REF = "market:";
	public static String ITEM_USER = "item_user:"; // 发布物品后跟用户弄一个关联
	public static String FUNDS = "funds";
	public static String USER_NAME = "name";

	/**
	 * <p>
	 * 摆货上架： 1、首先校验该用户对应着是否有库存 2、从加入itemid和price到market中 3、ITEM_USER 通过建立起itemid
	 * 和 所属用户的关系 4、删除库存中对应itemid
	 * <p>
	 * 
	 * @param seller
	 * @param itemid
	 * @param price
	 * @return
	 */
	public boolean listItem(String seller, String itemid, double price) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();

			String inventory = INVENTORY_REF + seller;
			conn.watch(inventory);
			if (!conn.sismember(inventory, itemid)) {
				conn.unwatch();
				return false;
			}

			Pipeline pipeline = conn.pipelined();
			pipeline.multi();
			pipeline.zadd(MARKET_REF, price, itemid);
			pipeline.hset(ITEM_USER, itemid, seller);
			pipeline.srem(inventory, itemid);
			Response<List<Object>> list = pipeline.exec();
			if (list != null) {
				return true;
			} else {
				return false;
			}
			
		} finally {
			this.realeseJedis(conn);
		}
	}
	
	/**
	 * <p>
	 * 货物交易：
	 * 1、对货物价格，买家资金，货物对应卖家进行校验
	 * 2、事务执行批量操作
	 * 	1）market容器中少一条item记录
	 * 	2） 买家对应得货物库存增加一条记录
	 * 	3）买家资金减少
	 * 	4）卖家资金增加
	 * 	5）清楚ITEM_USER中item跟用户得关系
	 * <p>
	 * @param buyer
	 * @param itemid
	 * @param lprice
	 * @return
	 */
	public boolean purchaseItem(String buyer, String itemid, double lprice){
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			String inventoryKey = INVENTORY_REF + buyer;
			String buyerKey = USER_REF + buyer;
			
			conn.watch(MARKET_REF, buyerKey, ITEM_USER);
			double price = conn.zscore("market:", itemid);
			double funds = Double.parseDouble(conn.hget(buyerKey, FUNDS));
			String seller = conn.hget(ITEM_USER, itemid);
			if (price != lprice || price > funds || seller == null) {
				conn.unwatch();
				return false;
			}
			String sellerKey = USER_REF + seller;
			
			Pipeline pipeline = conn.pipelined();
			pipeline.multi();
			pipeline.zrem(MARKET_REF, itemid);
			pipeline.sadd(inventoryKey, itemid);
			pipeline.hincrBy(buyerKey, FUNDS, (int)-lprice); //调用hincrBy生效的初始化Funds的时候是Integer, 这里传得参数也必须为int，所以实际使用场合中这个方法不可取
			pipeline.hincrBy(sellerKey, FUNDS, (int)lprice);
			pipeline.hdel(ITEM_USER, itemid);
			Response<List<Object>> list = pipeline.exec(); 
			
			if(list != null){
				return true;
			}
			
			return false;
			
		} finally {
			this.realeseJedis(conn);
		}
	}

}
