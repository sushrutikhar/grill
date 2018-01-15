package org.apache.lens.driver.job.utils;

import lombok.Data;
import org.apache.lens.driver.job.states.JobState;
import org.apache.lens.server.api.error.LensException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;

@Data
public class JobUtils {

    private String baseUrl = "https://yoda.azuredatalakeanalytics.net/";

    public static void submitJob(String jobId, String payload, String bearerToken){

    }

    public static JobState getStatus(String jobId, String bearerToken) {
        return JobState.COMPLETED;
    }

    public static InputStream getResult(String jobId, String bearerToken) throws LensException{
        try {
            return new FileInputStream("/tmp/dummy.csv");
        } catch (FileNotFoundException e) {
            throw new LensException(e);
        }
    }

}
