package cn.zwz.component;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RedisSemaphoreTest {


	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		long limit = 2;
		long timeout = 1000;
		String semName = "dxy";
		
		RedisSemaphore sem = new RedisSemaphore();
//		String ar1 =  sem.acquireSemaphore(semName, timeout, limit);
//		
//		String ar2 = sem.acquireSemaphore(semName, timeout, limit);
//		
//		String ar3 = sem.acquireSemaphore(semName, timeout, limit);
//		assertEquals(null, ar3);
		
		String ar11 =  sem.acquireFairSemaphore(semName, timeout, limit);
		
		String ar21 = sem.acquireFairSemaphore(semName, timeout, limit);
		
		String ar31 = sem.acquireFairSemaphore(semName, timeout, limit);
		
	}

}
