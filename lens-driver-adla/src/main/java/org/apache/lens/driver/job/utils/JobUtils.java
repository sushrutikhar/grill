/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.job.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.driver.job.states.JobState;
import org.apache.lens.server.api.error.LensException;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.json.JSONException;
import org.json.JSONObject;


public class JobUtils {

  private JobUtils() {}

  private static String baseUrl = "https://yoda.azuredatalakeanalytics.net/";

  private static Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));

  private static String querySkeleton = "{  \n"
    + "  \"jobId\": \"<<jobid>>\",  \n"
    + "  \"name\": \"<<jobid>>\",  \n"
    + "  \"type\": \"USql\",  \n"
    + "  \"degreeOfParallelism\": 1,  \n"
    + "  \"priority\": 1000,  \n"
    + "  \"properties\": {  \n"
    + "    \"type\": \"USql\",  \n"
    + "    \"script\": \"<<script>>\"  \n"
    + "  }  \n"
    + "}  ";

  public static void submitJob(String jobId, String payload, String bearerToken) throws LensException {
    System.out.println(bearerToken.length());
    payload = payload.replace("\"", "\\\"");
    String finalquery = querySkeleton.replace("<<script>>", payload);
    finalquery = finalquery.replace("<<jobid>>", jobId);
    String requestUrl = baseUrl + "jobs/" + jobId + "?api-version=2016-11-01";
    WebTarget webResource = client.target(requestUrl);
    Invocation.Builder x = webResource.request(MediaType.APPLICATION_JSON);
    x = x.header("Authorization", bearerToken);
    x = x.accept("application/json");
    Response response = x.put(Entity.entity(finalquery, MediaType.APPLICATION_JSON));
    if (response.getStatus() != 200) {
      throw new LensException();
    }
    String output = response.readEntity(String.class);
    System.out.println(output);
    System.out.println(finalquery);
  }

  public static JobState getStatus(String jobId, String bearerToken) throws LensException {
    String requestUrl = baseUrl + "jobs/" + jobId + "?api-version=2016-11-01";
    WebTarget webResource = client.target(requestUrl);
    Invocation.Builder x = webResource.request(MediaType.APPLICATION_JSON);
    x = x.header("Authorization", bearerToken);
    x = x.accept("application/json");
    Response response = x.get();
    if (response.getStatus() != 200) {
      throw new LensException();
    }
    String output = response.readEntity(String.class);
    try {
      JSONObject jsonObject = new JSONObject(output);
      if (jsonObject.get("result") == null) {
        return JobState.DOES_NOT_EXIST;
      }
      if (jsonObject.get("result").toString().trim().equals("Succeeded")) {
        return JobState.COMPLETED;
      }
      if (jsonObject.get("Failed").toString().trim().equals("Running")) {
        return JobState.FAILED;
      }
      return JobState.RUNNING;
    } catch (JSONException e) {
      throw new LensException("Unknown error, unable to parse the result");
    }
  }

  public static InputStream getResult(String jobId, String bearerToken) throws LensException {
    try {
      return new FileInputStream("/tmp/dummy.csv");
    } catch (FileNotFoundException e) {
      throw new LensException(e);
    }
  }

/*  public static void main(String[] args) throws LensException {
    JobUtils.getStatus("aa96c2e1-8f55-4fa6-b95f-ae85b25a1a0f", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ino0NHdNZEh1OHdLc3VtcmJmYUs5OHF4czVZSSIsImtpZCI6Ino0NHdNZEh1OHdLc3VtcmJmYUs5OHF4czVZSSJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC84NzhmNmIzMS1mZWVhLTQyYmMtYTk1ZC1kNDQ5NmY5YmVmNjkvIiwiaWF0IjoxNTE2MDc3MTc2LCJuYmYiOjE1MTYwNzcxNzYsImV4cCI6MTUxNjA4MTA3NiwiYWNyIjoiMSIsImFpbyI6IlkyTmdZSWkxdWhLZ2xaYVI5cnByY2JIK2JMMzBhVDRCRlg4bWxMd1EzNlYza09YNDBob0EiLCJhbHRzZWNpZCI6IjE6bGl2ZS5jb206MDAwMzQwMDFBQzNDNzZGRiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiJjNDRiNDA4My0zYmIwLTQ5YzEtYjQ3ZC05NzRlNTNjYmRmM2MiLCJhcHBpZGFjciI6IjIiLCJlX2V4cCI6MjYyODAwLCJlbWFpbCI6InB1bmVldGd1cHRhQG91dGxvb2suY29tIiwiZmFtaWx5X25hbWUiOiJHdXB0YSIsImdpdmVuX25hbWUiOiJQdW5lZXQiLCJncm91cHMiOlsiNDQwZDg5MjctYTY0Yi00MjJiLWI2YzktNDE2NTIwYzA0ZGY1IiwiOGY1NGMwZGItMjYwZi00NWVjLTk1ZTUtMTc2MTkxYTIxNWNkIl0sImlkcCI6ImxpdmUuY29tIiwiaXBhZGRyIjoiMTQuMTQyLjEwNC4xNzAiLCJuYW1lIjoiUHVuZWV0IEd1cHRhIiwib2lkIjoiMmI4ZWZkYzEtZDQwMy00MjQwLWExZDktMjhhYjM1MTI3NDgyIiwicHVpZCI6IjEwMDMzRkZGQTc5NTNBMjAiLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiJnREZYZEVPbk43VjdEWDlhd0RQMVdKbmtERGxUYW53c0tJZEx0NU1hNG0wIiwidGlkIjoiODc4ZjZiMzEtZmVlYS00MmJjLWE5NWQtZDQ0OTZmOWJlZjY5IiwidW5pcXVlX25hbWUiOiJsaXZlLmNvbSNwdW5lZXRndXB0YUBvdXRsb29rLmNvbSIsInV0aSI6IlNqNUJJdVB1S2ttaV9HSW9BdndsQUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCJdfQ.o67mpIbcsnpR_SJOMGRY73NMXr4VfXXtAMOx6aA8OcV8f1VWzyB2Ovo2C6-TTySqmR01HHtT5A1phOWSEnngb3sGo26UnwAUeXCuloeZ2iuXeR0sSOcyYcAdwNn-5ELhy-oGDvYTt3z5fhNK0vhblqRl7xaob0-3epMnuPVw41i1sTT4fqh7OfqY50qr85lOZBeHF8ctEEc_iylt4wd3MKbG4JT12QTI_0Amuqiqsczbr2PtjHMp2BjSQiyRo0OjyKqvPrUKCrc0Uu0lDtgMwQvf2eVMT9k4CIvLqEE1UIXtnTg_ARzk-EsC8EZlkovi9MgkiyFJzacM5ZMKMXdNKw");
  }*/

}
