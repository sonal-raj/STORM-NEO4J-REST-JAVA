import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

class cpumemory
{
private static void compute() {
		long memorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
	        .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
		System.out.println("Total Memory : "+(memorySize/(1024*1024))+"Mb");
	    
		long freeMem = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
		        .getOperatingSystemMXBean()).getFreePhysicalMemorySize();
		System.out.println("Free Memory : "+(freeMem/(1024*1024))+"Mb");
		
		double cpuLoad = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
		        .getOperatingSystemMXBean()).getSystemCpuLoad();
		System.out.println("Total Memory : "+(cpuLoad*100)+"%");
} 
public static void main(String[] args) {
	compute();
}
} 