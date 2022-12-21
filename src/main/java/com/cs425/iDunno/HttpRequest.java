package com.cs425.iDunno;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpRequest {

    public static Boolean sendTrainingRequest() throws IOException {
        Boolean result = false;
        for(String api: constants.train_api) {
            int statusCode = 500;
            HttpResponse response = getRequest(api);
            if (response != null) {
                statusCode = response.getStatusLine().getStatusCode();
            }
            if (statusCode == 200) {
                result = true;
            }
        }
        return result;
    }

    public static HttpResponse getRequest(String api_path) {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpResponse response = null;
        try {
            HttpGet getRequest = new HttpGet(api_path);
            getRequest.addHeader("accept", "application/json");
            response = httpClient.execute(getRequest);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally
        {
            //Important: Close the connect
            httpClient.getConnectionManager().shutdown();
        }
        return response;
    }

    public static HttpResponse postRequest(String api_path, List<String> files){
        DefaultHttpClient httpClient = new DefaultHttpClient();
        HttpResponse response = null;
        try{
            HttpPost postRequest = new HttpPost(api_path);
            postRequest.addHeader("content-type", "application/json");
            Map<String, List<String>> map = new HashMap<>();
            map.put("filepaths", files);
            String json = new ObjectMapper().writeValueAsString(map);
            postRequest.setEntity(new StringEntity(json));
            response = httpClient.execute(postRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally
        {
            //Important: Close the connect
            httpClient.getConnectionManager().shutdown();
        }
        return response;
    }

        public static void sendQueryRequest(Query query) {
        // Generate Http POST request with filename and model name to local flask API for processing
            String apiPath = constants.BASE_API + query.getModelName() + "/test";
            DefaultHttpClient httpClient = new DefaultHttpClient();
            try {
                // Send request
                HttpPost postRequest = new HttpPost(apiPath);
                postRequest.addHeader("content-type", "application/json");
                Map<String, List<String>> map = new HashMap<>();
                map.put("filepaths", query.getQueryFiles());
                String json_map = new ObjectMapper().writeValueAsString(map);
                postRequest.setEntity(new StringEntity(json_map));
                HttpResponse response = httpClient.execute(postRequest);
                if (response != null) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == 200) {
                        String json = EntityUtils.toString(response.getEntity());
                        ObjectMapper objectMapper = new ObjectMapper();
                        ModelResponse response_model = objectMapper.readValue(json, ModelResponse.class);
                        // Wait for Http OK response & put result in query
                        query.setClassificationLabels(response_model.getResult());
                    }
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
            finally
            {
                //Important: Close the connect
                httpClient.getConnectionManager().shutdown();
            }
    }
}
