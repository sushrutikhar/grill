package org.apache.lens.driver.job.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.lens.driver.job.states.JobState;

import java.io.OutputStream;

@Data
@AllArgsConstructor
public class JobUtils {

    private String bearerToken;

    private String baseUrl;

    public void submitJob(String jobId, String payload){

    }

    public JobState getStatus(String jobId) {
        return JobState.COMPLETED;
    }

    public OutputStream getResult(String jobId) {
        return null;
    }

}
