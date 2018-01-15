package org.apache.lens.driver.adla;

import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.PersistentResultSet;
import org.apache.lens.server.api.error.LensException;

import java.io.File;

public class PersistentADLAResult extends PersistentResultSet{

    String path;
    public PersistentADLAResult(String path) {
        this.path = path;
    }

    @Override
    public Long getFileSize() throws LensException {
        return new File(path).getTotalSpace();
    }

    @Override
    public Integer size() throws LensException {
        return null;
    }

    @Override
    public LensResultSetMetadata getMetadata() throws LensException {
        return null;
    }

    @Override
    public String getOutputPath() throws LensException {
        return path;
    }
}
