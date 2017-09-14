package fastweb.udap.elasticsearch;

import java.util.List;

public interface IndexProvider {
	public List<String> getIndices(String metricName);
}
