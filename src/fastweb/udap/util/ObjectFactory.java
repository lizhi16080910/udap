package fastweb.udap.util;

import java.lang.reflect.Constructor;


/**
 * 名称: ObjectFactory.java
 * 描述: 类加载工厂
 * 最近修改时间: Oct 10, 201410:36:27 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class ObjectFactory {
	
	/**
	 * Instantiate a {@link Class} instance for given {@linkplain className}.
	 * @param object
	 * @return
	 */
	public static Object getInstance(String className) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true, ObjectFactory.class.getClassLoader());
			instance = clazz.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}
	
	/**
	 * Instantiate a {@link Class} instance for given parameters:
	 * {@linkplain clazz} and {@linkplain parameters}.
	 * @param clazz
	 * @param parameters
	 * @return
	 */
	public static Object getInstance(Class<?> clazz, Object... parameters) {
		Object instance = null;
		try {
			Constructor<?>[] constructors = clazz.getConstructors();
			for(Constructor<?> c : constructors) {
				if(c.getParameterTypes().length==parameters.length) {
					instance = c.newInstance(parameters);
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}
	
	/**
	 * Instantiate a {@link Class} instance for given parameters:
	 * {@linkplain className} and {@linkplain parameters}.
	 * @param className
	 * @param parameters
	 * @return
	 */
	public static Object getInstance(String className, Object... parameters) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true, ObjectFactory.class.getClassLoader());
			instance = getInstance(clazz, parameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}
	
}
