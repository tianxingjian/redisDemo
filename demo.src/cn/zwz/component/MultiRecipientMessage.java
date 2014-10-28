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
 * 由于 取消息方法没有弄明白，可能有bug
 * <p>
 * @author tianxingjian
 *
 */
public class MultiRecipientMessage extends PoolDao {
	
	private static String IDS_CHART = "ids:chart:";
	private static String CHART = "chart:";
	private static String SEEN = "seen:";
	private static String IDS = "ids:";
	private static String MSG = "msgs:";
	
	public String createChat(String sender, List<String>recepiters, String message, String chartId){
		Jedis conn = null;
		try{
			conn = this.getPoolManager().getJedis();
			if(chartId == null || chartId.trim().length() == 0){
				chartId = String.valueOf(conn.incr(IDS_CHART));
			}
			
			recepiters.add(sender);
			Transaction trans = conn.multi();
			for(String recepent : recepiters){
				trans.zadd(CHART + chartId, 0, recepent);
				trans.zadd(SEEN + recepent, 0, chartId);
			}
			trans.exec();
			
			return sendMessage(sender, message, chartId);
			
		}finally{
			this.realeseJedis(conn);
		}
	}

	private String sendMessage(String sender, String message, String chartId) {
		
		Jedis conn = null;
		
		try{
			DistributedLock lock = new DistributedLock();
			String identifider = null;
			try{
				identifider = lock.aquireLock(CHART + chartId);
				if(identifider == null){
					throw new RuntimeException("资源锁已经被占用！");
				}
				conn = this.getPoolManager().getJedis();
				
				long messageId = conn.incr(IDS + chartId);
				Map<String, Object> msgMap = new HashMap<String, Object>();
				msgMap.put("id", messageId);
				msgMap.put("ts", System.currentTimeMillis());
				msgMap.put("sender", sender);
				msgMap.put("msg", message);
				
				//转换成Json报文
				String packed = new Gson().toJson(msgMap);
				conn.zadd(MSG + chartId, messageId, packed);
			}finally{
				lock.realeaseLock(CHART + chartId, identifider);
			}
			return chartId;
		}finally{
			this.realeseJedis(conn);
		}
	}
	
	/**
	 * <p>
	 * 接收消息，这个方法没弄明白
	 * <p>
	 * @param conn
	 * @param recipient
	 * @return
	 */
	@SuppressWarnings("unchecked")
    public List<ChatMessages> fetchPendingMessages(Jedis conn, String recipient) {
        Set<Tuple> seenSet = conn.zrangeWithScores(SEEN + recipient, 0, -1);
        List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

        Transaction trans = conn.multi();
        for (Tuple tuple : seenList){
            String chatId = tuple.getElement();
            int seenId = (int)tuple.getScore();
            trans.zrangeByScore(MSG + chatId, String.valueOf(seenId + 1), "inf");
        }
        List<Object> results = trans.exec();

        Gson gson = new Gson();
        Iterator<Tuple> seenIterator = seenList.iterator();
        Iterator<Object> resultsIterator = results.iterator();

        List<ChatMessages> chatMessages = new ArrayList<ChatMessages>();
        List<Object[]> seenUpdates = new ArrayList<Object[]>();
        List<Object[]> msgRemoves = new ArrayList<Object[]>();
        while (seenIterator.hasNext()){
            Tuple seen = seenIterator.next();
            Set<String> messageStrings = (Set<String>)resultsIterator.next();
            if (messageStrings.size() == 0){
                continue;
            }

            int seenId = 0;
            String chatId = seen.getElement();
            List<Map<String,Object>> messages = new ArrayList<Map<String,Object>>();
            for (String messageJson : messageStrings){
                Map<String,Object> message = (Map<String,Object>)gson.fromJson(
                    messageJson, new TypeToken<Map<String,Object>>(){}.getType());
                int messageId = ((Double)message.get("id")).intValue();
                if (messageId > seenId){
                    seenId = messageId;
                }
                message.put("id", messageId);
                messages.add(message);
            }

            conn.zadd(CHART+ chatId, seenId, recipient);
            seenUpdates.add(new Object[]{SEEN + recipient, seenId, chatId});

            Set<Tuple> minIdSet = conn.zrangeWithScores(CHART + chatId, 0, 0);//这个是什么意思
            if (minIdSet.size() > 0){
                msgRemoves.add(new Object[]{
                    MSG + chatId, minIdSet.iterator().next().getScore()});
            }
            chatMessages.add(new ChatMessages(chatId, messages));
        }

        trans = conn.multi();
        for (Object[] seenUpdate : seenUpdates){
            trans.zadd(
                (String)seenUpdate[0],
                (Integer)seenUpdate[1],
                (String)seenUpdate[2]);
        }
        for (Object[] msgRemove : msgRemoves){//这个是什么意思
            trans.zremrangeByScore(
                (String)msgRemove[0], 0, ((Double)msgRemove[1]).intValue());
        }
        trans.exec();

        return chatMessages;
    }
	
	public void joinChart(String joiner, String chartId){
		Jedis conn = null;
		try{
			conn = this.getPoolManager().getJedis();
			long msgid = conn.incr(IDS + chartId);
			Transaction trans = conn.multi();
			trans.zadd(CHART + chartId, msgid, joiner);
			trans.zadd(SEEN + joiner, msgid, chartId);
			trans.exec();
		}finally{
			this.realeseJedis(conn);
		}
	}
	
	public void leaveChart(String leaver, String chartId){
		Jedis conn = null;
		try{
			conn = this.getPoolManager().getJedis();
			Transaction trans = conn.multi();
			trans.zrem(CHART + chartId, leaver); //删除会话中退出得leaver
			trans.zrem(SEEN + leaver, chartId); //从leaver可看见得会话删除chartId
			trans.zcard(CHART + chartId); //chartID查看还有多少人
			List<Object> list = trans.exec();
			
			int count = ((Long) list.get(list.size() - 1)).intValue();
			if(count < 1){ //会话中没人了就删除整个会话
				trans.del(MSG + chartId);
				trans.del(IDS + chartId);
				trans.del(CHART + chartId);
				trans.exec();
			}else{
				//删除旧消息，觉得redis in action 是错的
			}
		}finally{
			this.realeseJedis(conn);
		}
	}
	
    public class ChatMessages
    {
        public String chatId;
        public List<Map<String,Object>> messages;

        public ChatMessages(String chatId, List<Map<String,Object>> messages){
            this.chatId = chatId;
            this.messages = messages;
        }

        public boolean equals(Object other){
            if (!(other instanceof ChatMessages)){
                return false;
            }
            ChatMessages otherCm = (ChatMessages)other;
            return chatId.equals(otherCm.chatId) &&
                messages.equals(otherCm.messages);
        }
    }
}
