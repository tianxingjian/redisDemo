package cn.zwz.log;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import cn.zwz.util.LogLevel;
import cn.zwz.util.PoolDao;

/**
 * <p>
 * Redis记录应用程序日志：
 * 这里只是一个写日志的两个方法，其实可以在此基础上扩展做日志分析。当然也可以直接通过客户端干分析这事
 * <p>
 * @author tianxingjian
 *
 */
public class LogOperate extends PoolDao {

	public static final Collator COLLATOR = Collator.getInstance();

	public static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat(
			"EEE MMM dd HH:00:00 yyyy");
	private static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:00:00");
	static {
		ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	private static final int ZERO = 0;
	private static final int MAXRECENT = 100;

	/**
	 * <p>
	 * 记录日志
	 * <p>
	 * 
	 * @param name
	 *            日志名
	 * @param message
	 *            日志信息
	 * @param level
	 *            日志等级
	 * @param pipe
	 *            redis管道
	 */
	public void logRecent(String name, String message, LogLevel level,
			Pipeline pipe) {
		Jedis conn = null;

		try {

			if (pipe == null) {
				conn = this.getPoolManager().getJedis();
				pipe = conn.pipelined();
			}

			String destination = "recent:" + name + ':' + level.getName();
			pipe.lpush(destination, TIMESTAMP.format(new Date()) + ": "
					+ message);
			pipe.ltrim(destination, ZERO, MAXRECENT - 1);
			pipe.exec();

		} finally {
			if (conn != null) {
				this.realeseJedis(conn);
			}
		}
	}

	/**
	 *<p>
	 *logRecent日志记录的升级版，会记录最近两个时间段相同类型日志的出现频率
	 *<p>
	 * @param name 日志名称
	 * @param message 日志信息
	 * @param level 日志等级
	 */
	public void logCommon(String name, String message, LogLevel level) {

		Jedis conn = null;

		try {
			String destination = "common:" + name + ':' + level.getName();
			String startKey = destination + ":start";
			
			conn = this.getPoolManager().getJedis();
			
			conn.watch(startKey);
			String hourStart = ISO_FORMAT.format(new Date());
			String exitsed = conn.get(startKey);
			
			Pipeline pipe =  conn.pipelined();
			pipe.multi();
			
			//对前一个时间段的数据进行备份，用这种方法最多备份一个时间段
			if (exitsed != null && COLLATOR.compare(exitsed, hourStart) < 0) {
				//rename如果源键本身有值是会被覆盖的，这就是最多备份一个时间段的原因
				pipe.rename(destination, destination + ":last"); //将前一个时间段（以小时为单位）的数据destination改成destination:last
				pipe.rename(startKey, destination + ":pstart");//将前一个时间段（以小时为单位）的startKey改成startKey:pstart 
				pipe.set(startKey, hourStart);
			}
			pipe.zincrby(destination, 1,  message);
			
			//common log 自然也不能少logRecent，通过传过去pipe保证logCommon里头的redis操作和logRecent的redis操作再同一个事务
			logRecent(name, message, level, pipe);

		} finally {
			if (conn != null) {
				this.realeseJedis(conn);
			}
		}
	}
}
