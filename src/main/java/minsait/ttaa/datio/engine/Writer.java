package minsait.ttaa.datio.engine;


import minsait.ttaa.datio.common.Common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.teamPosition;
import static org.apache.spark.sql.SaveMode.Overwrite;

abstract class Writer {

    static void write(Dataset<Row> df) {
        df
                .coalesce(1)
                .write()
                .partitionBy("nationality")/*Se modifica por que mandaba error*/
                .mode("overwrite") /*Se modifica por que mandaba error*/
                .parquet(Common.OUTPUT_PATH);
    }

}
