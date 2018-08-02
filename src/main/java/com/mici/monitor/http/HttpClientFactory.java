package com.mici.monitor.http;

import com.mici.monitor.MonitorConfig;
import com.mici.monitor.exception.HttpClientInitException;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class HttpClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);


    public static HttpClient createHttpClient(MonitorConfig config) throws HttpClientInitException {
        Objects.requireNonNull(config);

        // 创建 ConnectingIOReactor
        ConnectingIOReactor ioReactor = initIOReactorConfig(config);

        // 创建链接管理器
        final PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);


        // 创建HttpAsyncClient
        CloseableHttpAsyncClient httpAsyncClient = createPoolingHttpClient(config,cm);

        // 组合生产HttpClientImpl
        HttpClient httpClientImpl = new HttpClient(config,httpAsyncClient);

        return httpClientImpl;
    }

    private static ConnectingIOReactor initIOReactorConfig(MonitorConfig config) {
        int ioThreadCount = config.getIoThreadCount();
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(ioThreadCount).build();
        ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
            return ioReactor;
        } catch (IOReactorException e) {
            throw new HttpClientInitException();
        }
    }

    private static CloseableHttpAsyncClient createPoolingHttpClient(MonitorConfig config,PoolingNHttpClientConnectionManager cm) throws HttpClientInitException {
        int httpConnectionPool = config.getHttpConnectionPool();
        int httpConnectionLiveTime = config.getHttpConnectionLiveTime();
        int httpKeepaliveTime = config.getHttpKeepaliveTime();

        RequestConfig requestConfig = initRequestConfig(config);

        if (httpConnectionPool > 0) {
            cm.setMaxTotal(httpConnectionPool);
            cm.setDefaultMaxPerRoute(httpConnectionPool);
            cm.closeExpiredConnections();
        }

        HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();

        // 设置连接管理器
        httpAsyncClientBuilder.setConnectionManager(cm);

        // 设置RequestConfig
        if (requestConfig != null) {
            httpAsyncClientBuilder.setDefaultRequestConfig(requestConfig);
        }

        // 设置Keepalive
        if (httpKeepaliveTime > 0) {
            MonitorConnectionKeepAliveStrategy hiTSDBConnectionKeepAliveStrategy = new MonitorConnectionKeepAliveStrategy(httpConnectionLiveTime);
            httpAsyncClientBuilder.setKeepAliveStrategy(hiTSDBConnectionKeepAliveStrategy);
        } else if (httpKeepaliveTime == 0) {
            MonitorConnectionReuseStrategy hiTSDBConnectionReuseStrategy = new MonitorConnectionReuseStrategy();
            httpAsyncClientBuilder.setConnectionReuseStrategy(hiTSDBConnectionReuseStrategy);
        }

        // 设置连接自动关闭
        if(httpConnectionLiveTime > 0) {
            MonitorHttpAsyncCallbackExecutor httpAsyncCallbackExecutor = new MonitorHttpAsyncCallbackExecutor(httpConnectionLiveTime);
            httpAsyncClientBuilder.setEventHandler(httpAsyncCallbackExecutor);
        }

        // 启动定时调度
        initFixedCycleCloseConnection(cm);

        CloseableHttpAsyncClient client = httpAsyncClientBuilder.build();
        return client;
    }

    private static RequestConfig initRequestConfig(MonitorConfig config) {
        RequestConfig requestConfig = null;

        // 设置请求
        int httpConnectTimeout = config.getHttpConnectTimeout();
        // 需要设置
        if (httpConnectTimeout >= 0) {
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
            // ConnectTimeout:连接超时.连接建立时间，三次握手完成时间.
            requestConfigBuilder.setConnectTimeout(httpConnectTimeout * 1000);
            // SocketTimeout:Socket请求超时.数据传输过程中数据包之间间隔的最大时间.
            requestConfigBuilder.setSocketTimeout(httpConnectTimeout * 1000);
            // ConnectionRequestTimeout:httpclient使用连接池来管理连接，这个时间就是从连接池获取连接的超时时间，可以想象下数据库连接池
            requestConfigBuilder.setConnectionRequestTimeout(httpConnectTimeout * 1000);
            requestConfig = requestConfigBuilder.build();
        }

        return requestConfig;
    }

    private static void initFixedCycleCloseConnection(final PoolingNHttpClientConnectionManager cm) {
        // 定时关闭所有空闲链接
        Executors.newSingleThreadScheduledExecutor(
                new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "Fixed-Cycle-Close-Connection" );
                        t.setDaemon(true);
                        return t;
                    }
                }

        ).scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    LOGGER.info("Close idle connections, fixed cycle operation");
                    cm.closeIdleConnections(3, TimeUnit.MINUTES);
                } catch(Exception ex) {
                    LOGGER.error("",ex);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private static class MonitorConnectionKeepAliveStrategy implements ConnectionKeepAliveStrategy {

        private long time;

        public MonitorConnectionKeepAliveStrategy(long time) {
            super();
            this.time = time;
        }

        @Override
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            return 1000 * time;
        }

    }

    private static class MonitorConnectionReuseStrategy implements ConnectionReuseStrategy {

        @Override
        public boolean keepAlive(HttpResponse response, HttpContext context) {
            return false;
        }

    }

}
