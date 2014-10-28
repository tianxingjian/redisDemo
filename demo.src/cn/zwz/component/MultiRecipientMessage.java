package cn.zwz.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import cn.zwz.util.PoolDao;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * <p>
 * 消息推送，代替publish SUBSCRIBE
 * 功能：模擬簡單的聊天室做消息推送
 * 三個重要的數據容器：
 * 1、chart:[chartId] 聊天室容器，一個聊天室一個ZSET，值記錄聊天室的人員，權值是人員讀過最新消息
 * 2、seen:[recepent] 某個人員可以看到的聊天室，值是對應得聊天室，權值是表示讀過該聊天室的最新消息
 * 3、msgs:[chartId] 聊天室chartID對應的消息，生成一條消息都會有權值，當消息被人員讀取後權值會更新到
 * 	  對應chart和seen。
 * 
 * 流程：
 * createChat：創建聊天室，初始化chart:[chartId] seen:[recepent] 並且發送創建消息，即第一條消息 
 * sendMessage：發送消息
 * fetchPendingMessages：人員消息讀取，讀取未讀消息，記錄讀到的位置（讀到的位置爲下次讀取的起點），刪除
 * 	所有人都讀到過的消息
 * joinChart：中途有人加入聊天室，更新chart:[chartId]，seen:[recepent] 容器
 * leaveChart：中途有人退出聊天室
 * <p>
 * 
 * @author tianxingjian
 *
 */
public class MultiRecipientMessage extends PoolDao {

	private static String IDS_CHART = "ids:chart:";
	private static String CHART = "chart:";
	private static String SEEN = "seen:";
	private static String IDS = "ids:";
	private static String MSG = "msgs:";

	public String createChat(String sender, List<String> recepiters,
			String message, String chartId) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			if (chartId == null || chartId.trim().length() == 0) {
				chartId = String.valueOf(conn.incr(IDS_CHART));
			}

			recepiters.add(sender);
			Transaction trans = conn.multi();
			for (String recepent : recepiters) {
				trans.zadd(CHART + chartId, 0, recepent);
				trans.zadd(SEEN + recepent, 0, chartId);
			}
			trans.exec();

			return sendMessage(sender, message, chartId);

		} finally {
			this.realeseJedis(conn);
		}
	}

	private String sendMessage(String sender, String message, String chartId) {

		Jedis conn = null;

		try {
			DistributedLock lock = new DistributedLock();
			String identifider = null;
			try {
				identifider = lock.aquireLock(CHART + chartId);
				if (identifider == null) {
					throw new RuntimeException("资源锁已经被占用！");
				}
				conn = this.getPoolManager().getJedis();

				long messageId = conn.incr(IDS + chartId);
				Map<String, Object> msgMap = new HashMap<String, Object>();
				msgMap.put("id", messageId);
				msgMap.put("ts", System.currentTimeMillis());
				msgMap.put("sender", sender);
				msgMap.put("msg", message);

				// 转换成Json报文
				String packed = new Gson().toJson(msgMap);
				conn.zadd(MSG + chartId, messageId, packed);
			} finally {
				lock.realeaseLock(CHART + chartId, identifider);
			}
			return chartId;
		} finally {
			this.realeseJedis(conn);
		}
	}

	/**
	 * <p>
	 * 接收者接收消息：
	 * 1、查詢接收者能所有看到的聊天室
	 * 2、每個聊天室從接收者上次查看過的後一條消息開始讀取最新消息（即查出該聊天室接收者未讀過的消息）
	 * 3、遍歷所有消息加入到結果集，並且更新聊天室接收者讀過的最新消息記錄（即更新聊天室權值），更新權值後
	 * 	  查看是否有記錄被所有人都讀取過，即權值是最小的值
	 * 4、更新seen：recipient讀到的最後一條消息記錄
	 * 5、刪除被所有接收者讀到過的記錄
	 * <p>
	 * 
	 * @param conn
	 * @param recipient
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<ChatMessages> fetchPendingMessages(Jedis conn, String recipient) {
		Set<Tuple> seenSet = conn.zrangeWithScores(SEEN + recipient, 0, -1); // 查询接收者可以接收到的聊天室记录
		List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

		Transaction trans = conn.multi();
		for (Tuple tuple : seenList) {
			String chatId = tuple.getElement();
			int seenId = (int) tuple.getScore();
			trans.zrangeByScore(MSG + chatId, String.valueOf(seenId + 1), "inf"); // 取出聊天室中接收者暂未读到得信息
		}
		List<Object> results = trans.exec();

		Gson gson = new Gson();
		Iterator<Tuple> seenIterator = seenList.iterator();
		Iterator<Object> resultsIterator = results.iterator();

		List<ChatMessages> chatMessages = new ArrayList<ChatMessages>();
		List<Object[]> seenUpdates = new ArrayList<Object[]>();
		List<Object[]> msgRemoves = new ArrayList<Object[]>();
		while (seenIterator.hasNext()) {
			Tuple seen = seenIterator.next();
			Set<String> messageStrings = (Set<String>) resultsIterator.next();
			if (messageStrings.size() == 0) {
				continue;
			}

			int seenId = 0;
			String chatId = seen.getElement();
			List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
			for (String messageJson : messageStrings) {
				Map<String, Object> message = (Map<String, Object>) gson
						.fromJson(messageJson,
								new TypeToken<Map<String, Object>>() {
								}.getType());
				int messageId = ((Double) message.get("id")).intValue();
				if (messageId > seenId) {
					seenId = messageId;
				}
				message.put("id", messageId);
				messages.add(message);
			}

			conn.zadd(CHART + chatId, seenId, recipient); // 取出信息的同时更新接收者已经读到哪条最新信息
			seenUpdates.add(new Object[] { SEEN + recipient, seenId, chatId }); // 后续用来更新接收者读到了哪条信息

			Set<Tuple> minIdSet = conn.zrangeWithScores(CHART + chatId, 0, 0);// 聊天室ZSET值是接收者，
			// 权值是读到的最后一条信息;按权值排名最小的那个权值对应得消息肯定是所有接收者都读过的

			if (minIdSet.size() > 0) {
				msgRemoves.add(new Object[] { MSG + chatId,
						minIdSet.iterator().next().getScore() }); // 记录所有接收者都读过得消息
			}
			chatMessages.add(new ChatMessages(chatId, messages));
		}

		trans = conn.multi();
		for (Object[] seenUpdate : seenUpdates) {// 更新接收者读到了某个聊天室得某条信息
			trans.zadd((String) seenUpdate[0], (Integer) seenUpdate[1],
					(String) seenUpdate[2]);
		}
		for (Object[] msgRemove : msgRemoves) {// 删除所有接收者都读过得消息
			trans.zremrangeByScore((String) msgRemove[0], 0,
					((Double) msgRemove[1]).intValue());
		}
		trans.exec();

		return chatMessages;
	}

	/**
	 * 加入聊天室：最新加入的人只能讀取下一條發送的消息，所以權值爲最新權值
	 * 可以改進的地方，加入時發送一條消息給全員
	 * @param joiner
	 * @param chartId
	 */
	public void joinChart(String joiner, String chartId) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			long msgid = conn.incr(IDS + chartId);
			Transaction trans = conn.multi();
			trans.zadd(CHART + chartId, msgid, joiner);
			trans.zadd(SEEN + joiner, msgid, chartId);
			trans.exec();
		} finally {
			this.realeseJedis(conn);
		}
	}

	/**
	 * 退出聊天室：
	 * @param leaver
	 * @param chartId
	 */
	public void leaveChart(String leaver, String chartId) {
		Jedis conn = null;
		try {
			conn = this.getPoolManager().getJedis();
			Transaction trans = conn.multi();
			trans.zrem(CHART + chartId, leaver); // 删除会话中退出得leaver
			trans.zrem(SEEN + leaver, chartId); // 从leaver可看见得会话删除chartId
			trans.zcard(CHART + chartId); // chartID查看还有多少人
			List<Object> list = trans.exec();

			int count = ((Long) list.get(list.size() - 1)).intValue();
			if (count < 1) { // 会话中没人了就删除整个会话
				trans.del(MSG + chartId);
				trans.del(IDS + chartId);
				trans.del(CHART + chartId);
				trans.exec();
			} else {
				// 一个人离开可能消息里头正好有一条消息只当前人未读过，此人离开就可以删除那条
				Set<Tuple> minIdSet = conn.zrangeWithScores(CHART + chartId, 0,
						0);
				List<Object[]> msgRemoves = new ArrayList<Object[]>();
				if (minIdSet.size() > 0) {
					msgRemoves.add(new Object[] { MSG + chartId,
							minIdSet.iterator().next().getScore() }); // 记录所有接收者都读过得消息
				}
				trans = conn.multi();

				for (Object[] msgRemove : msgRemoves) {// 删除所有接收者都读过得消息
					trans.zremrangeByScore((String) msgRemove[0], 0,
							((Double) msgRemove[1]).intValue());
				}
				trans.exec();
			}
		} finally {
			this.realeseJedis(conn);
		}
	}

	public class ChatMessages {
		public String chatId;
		public List<Map<String, Object>> messages;

		public ChatMessages(String chatId, List<Map<String, Object>> messages) {
			this.chatId = chatId;
			this.messages = messages;
		}

		public boolean equals(Object other) {
			if (!(other instanceof ChatMessages)) {
				return false;
			}
			ChatMessages otherCm = (ChatMessages) other;
			return chatId.equals(otherCm.chatId)
					&& messages.equals(otherCm.messages);
		}
	}
}
