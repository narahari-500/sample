package com.serverless;

import okhttp3.*;
import org.json.JSONObject;

import java.io.IOException;
import java.net.MalformedURLException;

@SuppressWarnings("Duplicates")
public class AppupCounter {

    int key = 0;

    Response response=null;

    public int appupInserter(String warc_url) {

        try {
            String url = "https://finder.appup.cloud/finder/crawler_test";
            RequestBody formBody = new FormBody.Builder()
                    .add("warc_url", warc_url)
                    .build();

            formBody.toString();
            Request request = new Request.Builder()
                    .url(url)
                    .post(formBody)
                    .build();

            OkHttpClient okHttpClient = new OkHttpClient();
            response = okHttpClient.newCall(request).execute();
            JSONObject responseContent=null;

            try
            {
                responseContent= new JSONObject(response.body().string());
            }
            catch (Exception e)
            {
                System.out.println(e);
            }

            key = (int) responseContent.get("GENERATED_KEY");

            response.body().close();
        } catch (MalformedURLException e) {

            System.out.println(e);
        } catch (IOException e) {
            System.out.println(e);
        }

        return key;
    }

    public String appupUpdater(int gen_key) {

        try {

            String url = "https://finder.appup.cloud/finder/crawler_test_counter";

            RequestBody formBody = new FormBody.Builder()
                    .add("gen_key", String.valueOf(gen_key))
                    .build();

            formBody.toString();

            Request request = new Request.Builder()
                    .url(url)
                    .put(formBody)
                    .build();
            OkHttpClient okHttpClient = new OkHttpClient();
            Response response = okHttpClient.newCall(request).execute();

            System.out.println(response);

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public String recordUpdater(int warc_id) throws IOException {

        Response response=null;
        String url = "https://finder.appup.cloud/finder/warc_record_updater";
        try {
            RequestBody formBody = new FormBody.Builder()
                    .add("warc_id", String.valueOf(warc_id))
                    .build();

            formBody.toString();
            Request request = new Request.Builder()
                    .url(url)
                    .put(formBody)
                    .build();
            OkHttpClient okHttpClient = new OkHttpClient();
            response = okHttpClient.newCall(request).execute();

            System.out.println(response.body().string());

        }  finally {
            response.body().close();
        }

        return null;
    }

    public String warcUrlUpdater(int warc_id) throws IOException {

        Response response=null;
        String url = "https://finder.appup.cloud/finder/warc_url_updater";
        try {

            RequestBody formBody = new FormBody.Builder()
                    .add("warc_id", String.valueOf(warc_id))
                    .build();

            formBody.toString();
            Request request = new Request.Builder()
                    .url(url)
                    .put(formBody)
                    .build();

            OkHttpClient okHttpClient = new OkHttpClient();
            response = okHttpClient.newCall(request).execute();

            System.out.println(response.body().string());
        }  finally {
            response.body().close();
        }
        return null;
    }

}


