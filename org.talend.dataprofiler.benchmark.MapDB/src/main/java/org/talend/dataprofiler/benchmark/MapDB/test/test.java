package org.talend.dataprofiler.benchmark.MapDB.test;

import com.sun.management.OperatingSystemMXBean;



public class test {

	public static void main(String strs[]){
		
		OperatingSystemMXBean bean =
				  (OperatingSystemMXBean)
				    java.lang.management.ManagementFactory.getOperatingSystemMXBean();
				long max = bean.getTotalPhysicalMemorySize();
				double formatMemoryInGB=max/1024.0/1024.0/1024.0;
				String   OS   =   System.getProperty( "sun.arch.data.model") ;
				System.out.println(OS);
	}
}
