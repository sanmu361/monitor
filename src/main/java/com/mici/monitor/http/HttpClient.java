package com.mici.monitor.http;

import com.mici.monitor.exception.HttpClientException;
import com.mici.monitor.MonitorConfig;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class HttpClient {
    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);
    private static Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private String host;
    private int port;
    private final CloseableHttpAsyncClient httpclient;
    private final AtomicInteger unCompletedTaskNum;


    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private final boolean httpCompress;


    HttpClient(MonitorConfig config,CloseableHttpAsyncClient httpclient){
        this.host = config.getHost();
        this.port = config.getPort();
        this.httpCompress = config.isHttpCompress();
        this.httpclient = httpclient;
        this.unCompletedTaskNum = new AtomicInteger(0);
    }

    public void close() throws IOException {
        this.close(false);
    }

    public void close(boolean force) throws IOException {
        if(!force){
            while(true){
                if(httpclient.isRunning()){
                    int i = this.unCompletedTaskNum.get();
                    if(i == 0){
                        break;
                    }else{
                        try {
                            Thread.sleep(50);
                            continue;
                        } catch (InterruptedException e) {
                            logger.warn("The thread {} is Interrupted", Thread.currentThread().getName());
                            continue;
                        }
                    }
                }else{
                    break;
                }
            }
        }
        httpclient.close();

    }

    private HttpResponse execute(HttpEntityEnclosingRequestBase request, String json) throws HttpClientException {
        if (json != null && json.length() > 0) {
            request.addHeader("Content-Type", "application/json");
            if (!this.httpCompress) {
                request.setEntity(generateStringEntity(json));
            } else {
                request.addHeader("Accept-Encoding", "gzip, deflate");
                request.setEntity(generateGZIPCompressEntity(json));
            }
        }

        unCompletedTaskNum.incrementAndGet();
        Future<HttpResponse> future = httpclient.execute(request, null);
        try {
            HttpResponse httpResponse = future.get();
            return httpResponse;
        } catch (InterruptedException e) {
            throw new HttpClientException(e);
        } catch (ExecutionException e) {
            throw new HttpClientException(e);
        } catch (UnsupportedOperationException e) {
            throw new HttpClientException(e);
        } finally {
            unCompletedTaskNum.decrementAndGet();
        }
    }

    private StringEntity generateStringEntity(String json) {
        StringEntity stringEntity = new StringEntity(json, Charset.forName("UTF-8"));
        return stringEntity;
    }

    private ByteArrayEntity generateGZIPCompressEntity(String json) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(baos);
            gzip.write(json.getBytes(DEFAULT_CHARSET));
        } catch (IOException e) {
            throw new HttpClientException(e);
        } finally {
            if(gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    throw new HttpClientException(e);
                }
            }
        }

        ByteArrayEntity byteEntity = new ByteArrayEntity(baos.toByteArray());
        byteEntity.setContentType("application/json");
        byteEntity.setContentEncoding("gzip");

        return byteEntity;
    }

    public HttpResponse post(String apiPath, String json) throws HttpClientException {
        return this.post(apiPath, json, new HashMap<String, String>());
    }

    public HttpResponse post(String apiPath, String json, Map<String, String> params) throws HttpClientException {
        String httpFullAPI = getUrl(apiPath);
        URI uri = createURI(httpFullAPI, params);
        final HttpPost request = new HttpPost(uri);
        return execute(request, json);
    }

    private URI createURI(String httpFullAPI, Map<String, String> params) {
        URIBuilder builder;
        try {
            builder = new URIBuilder(httpFullAPI);
        } catch (URISyntaxException e) {
            throw new HttpClientException(e);
        }

        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                builder.setParameter(entry.getKey(), entry.getValue());
            }
        }

        URI uri;
        try {
            uri = builder.build();
        } catch (URISyntaxException e) {
            throw new HttpClientException(e);
        }
        return uri;
    }

    private String getUrl(String apiPath) {
        return "http://" + host + ":" + port + apiPath;
    }

    public void start() {
        this.httpclient.start();
    }


}
