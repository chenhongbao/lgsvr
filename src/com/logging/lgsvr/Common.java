package com.logging.lgsvr;

import java.io.File;
import java.io.PrintStream;

public class Common {

	public Common() {
	}
	
	public static void PrintException(Exception e) {
		File f = new File("exception.log");
		try {
			if (!f.exists()) {
				f.createNewFile();
			}
			e.printStackTrace(new PrintStream(f));
		} catch (Exception e1) {
		}
	}

}
