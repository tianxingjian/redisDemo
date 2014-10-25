package cn.zwz.util;

/**
 * <p>
 * 日志等级枚举
 * <p>
 * @author tianxingjian
 *
 */
public enum LogLevel {
	
	DEBUG(0, "debug"), 
	INFO(1, "info"), 
	WARNING(2, "warning"), 
	ERROR(3, "error"), 
	CRITICAL(4, "critical");
	
	private LogLevel(int value, String name){
		this.value = value;
		this.name = name;
	}
	
	private int value;
	private String name;
	
	public int getValue() {
		return value;
	}
	public String getName() {
		return name;
	}
	
}
