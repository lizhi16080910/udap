package test;

import static org.junit.Assert.*;

import org.junit.Test;

import fastweb.udap.util.DatetimeUtil;



public class DatetimeUtilTest {

	@Test public void impalaStrToDateLong() throws Exception{
		
		String target = "20150701201512";
		String result = DatetimeUtil.impalaStrToDateLong(target);
		
		assertEquals("1435752912",result);
	}
	
}
