package org.apache.lens.driver.job.utils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.lens.driver.job.states.JobState;
import org.apache.lens.server.api.error.LensException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStream;

public class JobUtils {

    private static final String baseUrl = "https://yoda.azuredatalakeanalytics.net/";

    private static Client client = Client.create();

    private static final String querySkeleton = "{  \n" +
            "  \"jobId\": \"<<jobid>>\",  \n" +
            "  \"name\": \"<<jobid>>\",  \n" +
            "  \"type\": \"USql\",  \n" +
            "  \"degreeOfParallelism\": 1,  \n" +
            "  \"priority\": 1000,  \n" +
            "  \"properties\": {  \n" +
            "    \"type\": \"USql\",  \n" +
            "    \"script\": \"<<script>>\"  \n" +
            "  }  \n" +
            "}  ";

    public static void submitJob(String jobId, String payload, String bearerToken) throws LensException{
        System.out.println(bearerToken.length());
        String finalquery = querySkeleton.replace("<<script>>", payload);
        finalquery = finalquery.replace("<<jobid>>", jobId);
        String requestUrl = baseUrl + "jobs/" + jobId +"?api-version=2016-11-01";
        WebResource webResource = client.resource(requestUrl);
        WebResource.Builder x = webResource.header("Content-Type", "application/json");
        x = x.header("Authorization",bearerToken);
        x = x.accept("application/json");
        ClientResponse response = x.put(ClientResponse.class,finalquery);
        String output = response.getEntity(String.class);
        System.out.println(output);
        System.out.println(finalquery);
        if (response.getStatus() != 200) throw new LensException();
    }

    public static JobState getStatus(String jobId, String bearerToken) throws LensException {
        System.out.println(bearerToken.length());
        String requestUrl = baseUrl + "jobs/" + jobId +"?api-version=2016-11-01";
        WebResource webResource = client.resource(requestUrl);
        WebResource.Builder x = webResource.header("Content-Type", "application/json");
        x = x.header("Authorization",bearerToken);
        x = x.accept("application/json");
        ClientResponse response = x.get(ClientResponse.class);
        String output = response.getEntity(String.class);
        try {
            JSONObject jsonObject = new JSONObject(output);
            if (jsonObject.get("result") == null)
                return JobState.DOES_NOT_EXIST;
            if (jsonObject.get("result").toString().trim().equals("Succeeded"))
                return JobState.COMPLETED;
            if (jsonObject.get("result").toString().trim().equals("Running"))
                return JobState.RUNNING;
            return JobState.FAILED;
        } catch (JSONException e) {
            throw new LensException("Unknown error, unable to parse the result");
        }
    }

    public static InputStream getResult(String jobId, String bearerToken) {
        return null;
    }


}
