package com.wangwenjun.flume.sinks;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***************************************
 * @author:Alex Wang
 * @Date:2017/1/7 QQ:532500648
 ***************************************/
public class TestSink extends AbstractSink implements Configurable {

    private final static Logger LOG = LoggerFactory.getLogger(TestSink.class);

    @Override
    public void configure(Context context) {
        //NO-OP
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
            if (event != null) {
                LOG.info("Alex Wang Sink=>Headers:[{}],Body:[{}]", event.getHeaders(), new String(event.getBody()));
            } else {
                status = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Throwable ex) {
            transaction.rollback();
            throw new EventDeliveryException("occur some error.", ex);
        } finally {
            transaction.close();
        }
        return status;
    }
}
