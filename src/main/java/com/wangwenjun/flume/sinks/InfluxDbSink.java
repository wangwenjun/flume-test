package com.wangwenjun.flume.sinks;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/***************************************
 * @author:Alex Wang
 * @Date:2017/1/8 QQ:532500648
 * QQ交流群:286081824
 ***************************************/
public class InfluxDbSink extends AbstractSink implements Configurable {

    private final static Logger LOG = LoggerFactory.getLogger(InfluxDbSink.class);

    //http://master:8086/write?db=mydb
    private final static String INFLUX_DB_URL = "influxDbUrl";

    private String influxDbUrl;

    private HttpURLConnection openConnection() throws Throwable {
        HttpURLConnection conn;
        try {
            URL influxDbURL = new URL(influxDbUrl);
            conn = (HttpURLConnection) influxDbURL.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Accept", "*/*");
            conn.setRequestProperty("Accept-Encoding", "gzip, deflate");
            conn.setRequestProperty("Accept-Language", "zh-CN,zh;q=0.8,en;q=0.6");
            conn.setRequestProperty("Cache-Control", "no-cache");
            conn.setRequestProperty("Connection", "keep-alive");
            conn.setRequestProperty("Content-Type", "text/plain;charset=UTF-8");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36");
            conn.setUseCaches(false);
            conn.setDoOutput(true);
            conn.setDoInput(true);
        } catch (Throwable e) {
            LOG.error("Create the HTTP Connection failed.", e);
            throw e;
        }

        return conn;
    }

    @Override
    public void configure(Context context) {
        this.influxDbUrl = context.getString(INFLUX_DB_URL);
        LOG.info("The InfluxDbURL {}", influxDbUrl);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;

        try {
            transaction.begin();
            event = channel.take();
            if (event == null) {
                status = Status.BACKOFF;
            } else {
                HttpURLConnection conn = openConnection();
                conn.setRequestProperty("Content-Length", String.valueOf(event.getBody().length));
                try (DataOutputStream dos = new DataOutputStream(conn.getOutputStream())) {
                    dos.write(event.getBody());
                    dos.flush();
                }

                LOG.info("The data write to InfluxDb successful.");
                LOG.info("The Http Response code [{}] and message [{}]", conn.getResponseCode(), conn.getResponseMessage());

                try (InputStream in = conn.getInputStream(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    byte[] bytes = new byte[1024];
                    int len;
                    while ((len = in.read(bytes)) != -1) {
                        baos.write(bytes, 0, len);
                    }
                    LOG.info("The Http Response entity body {}", baos.toString("UTF-8"));
                }
            }
        } catch (Throwable throwable) {
            throw new EventDeliveryException("Save Data to InfluxDB have error.", throwable);
        } finally {
            transaction.commit();
            transaction.close();
        }
        return status;
    }
}