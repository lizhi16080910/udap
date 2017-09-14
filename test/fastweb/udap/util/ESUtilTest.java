package fastweb.udap.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import fastweb.udap.web.EsClientFactory;

public class ESUtilTest {

	@Test
	public void testIsIndexExistsStringClient() {
		assertTrue(ESUtil.isIndexExists("cdnlog.ua", EsClientFactory
				.createClient()));
	}

	@Test
	public void testIsIndexExistsString() {
		assertTrue(ESUtil.isIndexExists("cdnlog.ua"));
	}

	@Test
	public void testGetExistIndices() {
		List<String> indices = new ArrayList<String>();
		indices.add("cdnlog.ua");
		indices.add("cdnlog.ua.hour");
		indices.add("cdnlog.ua.po");
		List<String> result = ESUtil.getExistIndices(indices);
		for (String index : result) {
			System.out.println(index);
		}
	}
}
