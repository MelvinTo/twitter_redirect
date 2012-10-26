package me.tuhao.storm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Pattern p = Pattern.compile("(\\d{1,2})");
	    Matcher m = p.matcher("12 54bb 1aa 65");
	    StringBuffer s = new StringBuffer();
	    while (m.find())
	        m.appendReplacement(s, String.valueOf(3 * Integer.parseInt(m.group(1))));
	    System.out.println(s.toString());
	}

}
