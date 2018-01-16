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

import lombok.extern.slf4j.Slf4j;


@Slf4j
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
    log.info("Submitting job {}  with payload {} and bearerToken {}", jobId, payload, bearerToken);
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
      log.error("Filed to submit JOB on ADLA. Job ID {}", jobId);
      throw new LensException("Filed to submit JOB on ADLA. Job ID  " +jobId);
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
      if (jsonObject.get("result").toString().trim().equals("Failed")) {
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

}
