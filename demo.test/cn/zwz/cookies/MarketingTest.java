package cn.zwz.cookies;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import cn.zwz.util.PoolDao;

public class MarketingTest extends PoolDao{

	public static String USER_REF = "user:";
	public static String INVENTORY_REF = "inventory:";
	public static String MARKET_REF = "market:";
	public static String ITEM_USER = "item_user:"; // 发布物品后跟用户弄一个关联
	public static String FUNDS = "funds";
	public static String USER_NAME = "name";
	
	@Before
	public void setUp() throws Exception {
		
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			
			String keys[] = new String[]{"inventory:dxy", "user:dxy", "inventory:zwz", "user:zwz", "market:", "item_user:"};
			conn.del(keys);

			String userAName = "zwz";
			int userAFund = 100;
			
			String userBName ="dxy";
			int userBFund = 150;
			
			String []itemid = new String[]{"itemFA1", "itemFA2", "itemFB1", "itemFB2"};
			
			String userKeyA = USER_REF + userAName;
			String userKeyB = USER_REF + userBName;
			
			String inventoryKeyA = INVENTORY_REF + userAName;
			String inventoryKeyB = INVENTORY_REF + userBName;
			
			
			Pipeline pipeline = conn.pipelined();
			pipeline.multi();
			pipeline.hset(userKeyA, USER_NAME, userAName);
			pipeline.hset(userKeyA, FUNDS, String.valueOf(userAFund));
			pipeline.hset(userKeyB, USER_NAME, userBName);
			pipeline.hset(userKeyB, FUNDS, String.valueOf(userBFund));
			
			pipeline.sadd(inventoryKeyA, itemid[0]);
			pipeline.sadd(inventoryKeyA, itemid[1]);
			pipeline.sadd(inventoryKeyB, itemid[2]);
			pipeline.sadd(inventoryKeyB, itemid[3]);
			
			Response<List<Object>> list = pipeline.exec();
			
		} finally {
			this.realeseJedis(conn);
		}
		
	}

	@Test
	public void test() {
		Marketing makerting = new Marketing();
		makerting.listItem("zwz", "itemFA1", 90);
		makerting.listItem("zwz", "itemFA2", 70);
		
		boolean  aResult = makerting.purchaseItem("dxy", "itemFA1", 90);
		assertFalse(!aResult);
		boolean  bResult = makerting.purchaseItem("dxy", "itemFA2", 70);
		assertFalse(bResult);
	}

}
