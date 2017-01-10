package com.wangwenjun.flume.sources;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;

/***************************************
 * @author:Alex Wang
 * @Date:2017/1/8 QQ:532500648
 ***************************************/
public class PerformanceJmxSource extends AbstractSource implements Configurable, PollableSource {

    private final static Logger LOG = LoggerFactory.getLogger(PerformanceJmxSource.class);

    private final static String JMX_URL = "jmxUrl";

    private final static String APP_NAME = "app";

    private final static String MEMORY_UNIT = "memoryUnit";

    private final static String DEFAULT_MEMORY_UNIT = "bytes";

    private final static String MEMORY_KB_UNIT = "kb";
    private final static String MEMORY_BYTE_UNIT = DEFAULT_MEMORY_UNIT;
    private final static String MEMORY_MB_UNIT = "mb";
    private final static String MEMORY_GB_UNIT = "gb";

    private String jmxUrl;

    private String appName;

    private String memoryUnit = DEFAULT_MEMORY_UNIT;

    @Override
    public synchronized void start() {
        super.start();
        LOG.info("com.wangwenjun.flume.sources.PerformanceJmxSource START.");
    }

    @Override
    public synchronized void stop() {
        super.stop();
        LOG.info("com.wangwenjun.flume.sources.PerformanceJmxSource STOP.");
    }


    @Override
    public void configure(Context context) {
        this.jmxUrl = context.getString(JMX_URL);
        this.appName = context.getString(APP_NAME);
        if (context.containsKey(MEMORY_UNIT)) {
            this.memoryUnit = context.getString(MEMORY_UNIT);
        }
        LOG.info("JmxUrl:{},AppName:{},memoryUnit:{}", new Object[]{jmxUrl, appName, memoryUnit});
    }

    @Override
    public Status process() throws EventDeliveryException {
        Map<String, String> headers = new HashMap<>();
        headers.put("timeStamp", String.valueOf(System.currentTimeMillis()));
        Event event = new SimpleEvent();
        event.setHeaders(headers);

        try {
            StringBuilder builder = new StringBuilder("Performance,App=");
            builder.append(appName).append(" ");

            JMXServiceURL jmxServiceURL = new JMXServiceURL(jmxUrl);
            JMXConnector connector = JMXConnectorFactory.connect(jmxServiceURL);
            MBeanServerConnection conn = connector.getMBeanServerConnection();
            ObjectName memoryUsageObject = new ObjectName("java.lang:type=Memory");

            CompositeData memoryUsage = (CompositeData) conn.getAttribute(memoryUsageObject, "HeapMemoryUsage");

            String normalizedMemory = normalizedMemory(memoryUsage);
            builder.append("memory").append("=").append(normalizedMemory).append(",");

            ObjectName cpuUsageObject = new ObjectName("java.lang:type=OperatingSystem");

            Double processCpuLoad = (Double) conn.getAttribute(cpuUsageObject, "ProcessCpuLoad");

            builder.append("cpu").append("=").append(processCpuLoad * 100);

            event.setBody(builder.toString().getBytes());
            LOG.info("The data [{}]", builder.toString());
            getChannelProcessor().processEvent(event);
            LOG.info("The data write successful.");
            Thread.sleep(1000 * 30L);
            return Status.READY;
        } catch (Throwable ex) {
            LOG.error("Error:", ex);
            return Status.BACKOFF;
        }
    }

    private String normalizedMemory(CompositeData memoryUsage) {
        Long used = (Long) memoryUsage.get("used");
        switch (memoryUnit) {
            case MEMORY_KB_UNIT:
                return String.valueOf(used / 1024L);
            case MEMORY_MB_UNIT:
                return String.valueOf(used / 1024L / 1024L);
            case MEMORY_GB_UNIT:
                return String.valueOf(used / 1024L / 1024L / 1024L);
            case MEMORY_BYTE_UNIT:
            default:
                return String.valueOf(used);
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 30 * 1000L;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 30 * 1000L;
    }
}