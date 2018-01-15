
package org.apache.lens.driver.adla;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;

import org.apache.lens.cube.query.cost.FactPartitionBasedQueryCostCalculator;
import org.apache.lens.driver.job.utils.JobUtils;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.QueryCostCalculator;
import org.apache.lens.server.api.query.cost.StaticQueryCost;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;


import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.lens.server.api.query.priority.CostRangePriorityDecider;
import org.apache.lens.server.api.query.priority.CostToPriorityRangeConf;

@Slf4j
@AllArgsConstructor
public class ADLADriver extends AbstractLensDriver {

    public JobUtils jobUtils;

    String localOutputPath;

    @Override
    public void configure(Configuration conf, String driverType, String driverName) throws LensException {
        super.configure(conf, driverType, driverName);

        localOutputPath = conf.get(LensConfConstants.DRIVER_OUTPUT_LOCAL_PATH);
        log.info("ADLA driver {} configured successfully", getFullyQualifiedName());
    }

    @Override
    public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
        return new StaticQueryCost(1.0d);
    }

    @Override
    public DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
        throw new LensException("UnSupported operation explain");
    }

    @Override
    public void prepare(PreparedQueryContext pContext) throws LensException {
        throw new LensException("UnSupported operation prepare");
    }

    @Override
    public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
        throw new LensException("UnSupported operation explainAndPrepare");
    }

    @Override
    public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
        throw new LensException("UnSupported operation closePreparedQuery");
    }

    @Override
    public LensResultSet execute(QueryContext context) throws LensException {
        throw new LensException("UnSupported operation execute");
    }

    @Override
    public void executeAsync(QueryContext context) throws LensException {
        log.info("Running query {} ", context.getQueryHandleString());
        //Submit ADLA JOB
    }

    @Override
    public void updateStatus(QueryContext context) throws LensException {
        //Update status of ADLA JOB
        log.info("Updating status for query {} ", context.getQueryHandleString());
        if(System.currentTimeMillis() - context.getSubmissionTime() >10000) {
            context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.SUCCESSFUL);
        }
    }

    @Override
    public void closeResultSet(QueryHandle handle) throws LensException {
        //NO OP
    }

    @Override
    public boolean cancelQuery(QueryHandle handle) throws LensException {
        return false;
    }

    @Override
    public void closeQuery(QueryHandle handle) throws LensException {
        //NO OP
    }

    @Override
    public void close() throws LensException {
        //NO OP
    }

    @Override
    public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {
        // NO OP
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // NO OP
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // NO OP
    }

    @Override
    protected LensResultSet createResultSet(QueryContext ctx) throws LensException {
        //Get JOB result
        log.info("Creating resultset for query {}", ctx.getQueryHandleString());

        OutputStream outputStream = jobUtils.getResult(ctx.getQueryHandle().getHandleIdString(), "");

        //Get JOB result
        return null;
    }

}