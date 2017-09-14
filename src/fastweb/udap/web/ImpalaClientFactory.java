package fastweb.udap.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class ImpalaClientFactory {

	public final static Connection createClient() {
		String ip = "30";// 21
		// Random rank = new Random();
		// ip = String.valueOf(rank.nextInt(10)+21);
		// System.out.println(ip);
		Connection conn = null;
		try {
			conn = DriverManager.getConnection("jdbc:hive2://192.168.100." + ip
					+ ":21050/default;auth=noSasl");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return conn;
	}
}
