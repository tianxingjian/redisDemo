package cn.zwz.util;

public class HostAndPortConfig {
	
	private static HostAndPort  defaultHost = new HostAndPort ("127.0.0.1" , 6379);
	
	public static HostAndPort getDefaultHostPort(){
		return defaultHost;
	}
	
}
