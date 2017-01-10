package com.wangwenjun.flume.interceptors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/***************************************
 * @author:Alex Wang
 * @Date:2017/1/7 QQ:532500648
 ***************************************/
public class TestInterceptor implements Interceptor {

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String tmp = new String(body);
        if (null != tmp) {
            tmp += "Test Interceptor";
            event.setBody(tmp.getBytes());
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        list.forEach(this::intercept);
        return list;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TestInterceptor();
        }

        @Override
        public void configure(Context context) {
            //NO-OP
        }
    }
}