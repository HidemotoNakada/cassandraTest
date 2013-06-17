package test;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class testJSON {

	static String json = "{\"name\": \"ExternalProcessUDF\", \"command\" : [\"wc\"], \"args\" : {\"saru\" : 1 }  }"; 
	//static String json = "{\"test\" : 1}";
	
	public static void main(String[] args) throws ParseException, InstantiationException, IllegalAccessException, NoSuchFieldException, SecurityException {
		JSONParser parser = new JSONParser();
		JSONObject o = (JSONObject)parser.parse(json);
//		Object val = o.get("test");
		External e = (External) unmarshal(o, External.class); 
		
//		Object val = o.get("command");
		System.out.println(e.name);
		System.out.println(e.command.get(0));
		System.out.println(e.args.saru);
	}
	static class Inner {
		public Long saru;
	}
	
	static class External {
		public String name;
		public List<String> command;
		public Inner args; 
	}
	
	
	static Object unmarshal(Object obj, Class clazz) throws InstantiationException, IllegalAccessException, NoSuchFieldException, SecurityException {
		if (clazz == String.class)
			return (String)obj;
		if (clazz == Integer.class)
			return (Integer)obj;
		if (clazz == Long.class)
			return (Long)obj;
		if (clazz == List.class) 
			return (JSONArray)obj;
		
		JSONObject jObj = (JSONObject)obj;
		Object o = clazz.newInstance();
		
		for (Object s: jObj.keySet()) {
			Field f =  clazz.getField((String)s);
			Object inner = unmarshal(jObj.get(s), f.getType());
			f.set(o, inner);
		}
		return o;
	}

}
