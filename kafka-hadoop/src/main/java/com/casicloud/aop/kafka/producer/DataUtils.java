package com.casicloud.aop.kafka.producer;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataUtils {

	public static final String [] keys=new String[]{"maxup","status","maxdown","minup","mindown"};
	public static List<Map<Object, Object>> grenData(Long num) {
		String [] iotids=new String[]{"iot0001","iot0002","iot0003","iot0004","iot0005"};
		String [] equipments=new String[]{"plc0001","plc0002","plc0003","plc0004","plc0005","plc0006","plc0007","plc0008","plc0009","plc00010"};
		DecimalFormat    df   = new DecimalFormat("######0.00");
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Random random=new Random();
		List<Map<Object, Object>> datas=new ArrayList<Map<Object,Object>>();
		for (Long i=0l ;i<num;i++) {
			String equipment=equipments[Math.abs(Double.valueOf((Math.random()*10)).intValue())];
			Map<Object, Object> data=new HashMap<Object, Object>();
			data.put("iotid", iotids[Math.abs(Double.valueOf((Math.random()*10-5)).intValue())]);
			data.put("equipment", equipment);
			data.put("name", equipment);
			data.put("type", "0"+String.valueOf(Math.abs(Double.valueOf((Math.random()*10-5)).intValue())));
			data.put("k", keys[Math.abs(Double.valueOf((Math.random()*10-5)).intValue())]);
			data.put("v", df.format(Math.random()*100));
			data.put("t", String.valueOf(System.currentTimeMillis()+random.nextInt(60*60*24*1000)));
			//System.out.println(data);
			System.out.println(sdf.format(new Date(System.currentTimeMillis()+random.nextInt(60*60*24*1000))));
			datas.add(data);	
		}
		return datas;
		
	}
}
