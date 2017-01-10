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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/***************************************
 * @author:Alex Wang
 * @Date:2017/1/7 QQ:532500648
 * QQ交流群:286081824
 ***************************************/
public class TestSource extends AbstractSource implements Configurable, PollableSource {

    private final static Logger LOG = LoggerFactory.getLogger(TestSource.class);

    private final static String INPUT_FILE = "input.file";

    private String inputFile;

    @Override
    public void configure(Context context) {
        this.inputFile = context.getString(INPUT_FILE);
    }

    @Override
    public synchronized void start() {
        super.start();
        LOG.info("com.wangwenjun.flume.sources.TestSource START.");
    }

    @Override
    public synchronized void stop() {
        super.stop();
        LOG.info("com.wangwenjun.flume.sources.TestSource STOP.");
    }

    @Override
    public Status process() throws EventDeliveryException {

        Map<String, String> headers = new HashMap<>();
        headers.put("type", "plainData");
        headers.put("path", inputFile);
        headers.put("timeStamp", String.valueOf(System.currentTimeMillis()));

        Path file = Paths.get(inputFile);
        if (!Files.exists(file))
            return Status.BACKOFF;

        try (InputStream in = Files.newInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            Event event = new SimpleEvent();
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }

            event.setHeaders(headers);
            event.setBody(builder.toString().getBytes());
            getChannelProcessor().processEvent(event);

            Files.copy(file, Paths.get(inputFile + ".COMPLETED"));
            Files.delete(file);
            return Status.READY;
        } catch (Throwable e) {
            LOG.error("Error:", e);
            return Status.BACKOFF;
        }

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 1000;
    }
}
