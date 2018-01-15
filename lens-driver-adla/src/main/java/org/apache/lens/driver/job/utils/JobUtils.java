package org.apache.lens.driver.job.utils;

import lombok.Data;
import org.apache.lens.driver.job.states.JobState;

import java.io.OutputStream;

@Data
public class JobUtils {

    private String baseUrl = "https://yoda.azuredatalakeanalytics.net/";

    public static void submitJob(String jobId, String payload, String bearerToken){

    }

    public static JobState getStatus(String jobId, String bearerToken) {
        return JobState.COMPLETED;
    }

    public static OutputStream getResult(String jobId, String bearerToken) {
        return null;
    }

}
