package fastweb.udap.util;

import java.io.File;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 名称: PropertiesConfig.java
 * 描述: 配置文件读取类
 * 最近修改时间: Oct 10, 201410:36:37 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class PropertiesConfig {

	private static Log log = LogFactory.getLog(PropertiesConfig.class);

	private static PropertiesConfig pc = null;
	private static Configuration conf = null;
	private static boolean configLoaded = false;

	private static String SystemPath = null;
	private static String CONFIG_FILE = "fastweb_udap.properties";

	static {
		getInstance();
	}

	private PropertiesConfig() {
	}

	/**
	 * Get a singleton PropertiesConfig object, Do not need to reload the
	 * configuration
	 * 
	 * @return
	 */
	public static PropertiesConfig getInstance() {
		return getInstance(null, false);
	}

	/**
	 * Get a singleton PropertiesConfig object
	 * 
	 * @param configFile
	 * @param isReload,
	 *                if you need to reload the configuration
	 * @return
	 */
	@SuppressWarnings("static-access")
	public static PropertiesConfig getInstance(String configFile,
			boolean isReload) {
		if (pc == null) {
			pc = new PropertiesConfig();
		}

		try {
			if (isReload) {
				pc.loadConfig(configFile);
			} else {
				if (!configLoaded) {
					pc.loadConfig(configFile);
				}
			}
			return pc;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return pc;
	}

	/**
	 * Get current workspace path
	 * 
	 * @return
	 */
	public static String getSystemPath() {
		if (SystemPath == null) {
			SystemPath = new File(PropertiesConfig.class.getClassLoader()
					.getResource(CONFIG_FILE).getFile()).getParent();
		}
		return SystemPath + "/";
	}

	/**
	 * Get current workspace path by config file
	 */
	public static String getSystemPathByconfigFile(String configFile,
			String defaultPath) {
		String systemPath;
		try {
			systemPath = new File(PropertiesConfig.class.getClassLoader()
					.getResource(configFile).getFile()).getCanonicalPath();
		} catch (Exception e) {
			return defaultPath;
		}
		return systemPath;
	}

	/**
	 * Load configuration file
	 * 
	 * @param configFile
	 */
	private synchronized static void loadConfig(String configFile) {
		if (PropertiesConfig.configLoaded) {
			return;
		}

		String configFilePath = "";
		getSystemPath();

		if (configFile == null || configFile.equals("")) {
			configFilePath = SystemPath + "/" + CONFIG_FILE;
		} else {
			int slash = configFile.indexOf("/");
			if (slash < 0) {
				configFilePath = SystemPath + "/" + configFile;
			} else {
				configFilePath = configFile;
			}
		}

		log.info("configFilePath=" + configFilePath);

		try {
			conf = new PropertiesConfiguration(configFilePath);
		} catch (Exception e) {
			log.warn("No config file found at " + configFilePath
					+ " or error occured when parsing.");
			return;
		}
		// configLoaded = true;
	}

	/**
	 * Load configuration file
	 * 
	 * @param configFile
	 * @return
	 */
	public static Configuration readConf(String configFile) {
		Configuration config = null;
		try {
			config = new PropertiesConfiguration(configFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return config;
	}

	/**
	 * Reads the configuration of the configuration file
	 * 
	 * @param key
	 * @param defaultValue
	 * @return int
	 */
	public static int getIntProperty(String key, int defaultValue) {
		if (StringUtil.isEmpty(key)) {
			return defaultValue;
		} else {
			try {
				return conf.getInt(key);
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	/**
	 * Reads the configuration of the configuration file
	 * 
	 * @param key
	 * @param defaultValue
	 * @return string
	 */
	public static String getStringProperty(String key, String defaultValue) {
		if (StringUtil.isEmpty(key)) {
			return defaultValue;
		} else {
			try {
				String result = conf.getString(key);
				if (null == result) {
					return defaultValue;
				}
				return result;
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	/**
	 * Reads the configuration of the configuration file
	 * 
	 * @param key
	 * @param defaultValue
	 * @return double
	 */
	public static double getDoubleProperty(String key, double defaultValue) {
		double value = defaultValue;
		if (!StringUtil.isEmpty(key)) {
			String result = conf.getString(key);
			if (!StringUtil.isEmpty(result)) {
				try {
					value = Double.parseDouble(result);
				} catch (NumberFormatException e) {
				}
			}
		}
		return value;
	}

	/**
	 * Reads the configuration of the configuration file
	 * 
	 * @param key
	 * @param defaultValue
	 * @return boolean
	 */
	public static boolean getBooleanProperty(String key, boolean defaultValue) {
		if (StringUtil.isEmpty(key)) {
			return defaultValue;
		} else {
			try {
				return conf.getBoolean(key);
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	/**
	 * Reads the configuration of the configuration file
	 * 
	 * @param key
	 * @param defaultValue
	 * @return string
	 */
	public static String getRegexStringProperty(String key, String defaultValue) {
		if (StringUtil.isEmpty(key)) {
			return defaultValue;
		} else {
			try {
				Configuration conf = readConf(getSystemPath() + CONFIG_FILE);
				return conf.getProperty(key).toString();
			} catch (Exception e) {
				return defaultValue;
			}
		}
	}

	public static void main(String[] args) {
		System.out.println(PropertiesConfig.getIntProperty(
				"eas.deploy.position", 0));
		System.out.println(PropertiesConfig.getStringProperty(
				"eas.data.export.path", ""));

	}
}
