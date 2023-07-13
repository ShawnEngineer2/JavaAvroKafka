package poc.http;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
public class Client {

    private String serverURL;

    public Client(String serverURL) {
        this.serverURL = serverURL;
    }

    public String get(String endpoint) {

        String output = "";

        try {
            URL url = new URL(this.serverURL + endpoint);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();

            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setRequestProperty("Accept", "application/json");

            if(httpURLConnection.getResponseCode() != 200) {
                throw new RuntimeException("Request Failed! HTTP Error Code : " + httpURLConnection.getResponseCode() + " HTTP Response : " + httpURLConnection.getResponseMessage());
            }

            InputStreamReader httpResponseBody = new InputStreamReader(httpURLConnection.getInputStream());
            BufferedReader responseBuffer = new BufferedReader(httpResponseBody);

            String responseData = "";

            while((responseData = responseBuffer.readLine()) != null) {
                output += responseData;
            }

            httpURLConnection.disconnect();

        } catch (Exception e) {
            System.out.println("Http Client Exception!!");
            e.printStackTrace();
        }

        return output;
    }

}
