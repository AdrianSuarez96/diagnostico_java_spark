package minsait.ttaa.datio.common.naming;

import minsait.ttaa.datio.common.Common;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.omg.CORBA.PUBLIC_MEMBER;

import static minsait.ttaa.datio.common.naming.PlayerInput.heightCm;
import static minsait.ttaa.datio.common.naming.PlayerInput.teamPosition;
import static org.apache.spark.sql.functions.*;
import static minsait.ttaa.datio.common.Common.*;

public final class PlayerOutput {

    public static Field catHeightByPosition = new Field("cat_height_by_position");

    //Metodo que selecciona las columnas del ejecicio 1 1, recibe y retorna un dataset
    public Dataset<Row> selectCols(Dataset<Row> input) {
        return input.select(OUTPUT_COLS);
    }

    //El metodo que recibe y regresa un dataset con la regla del ejecicio 2
    public Dataset<Row> getAgeRange(Dataset<Row> input){
        return input.withColumn(AGE_RANGE_COl,
                when(AGE_COLUMN.$less( 23 ) , AGE_RANGE_A )
                        .when( AGE_COLUMN.$less( 27 ) , AGE_RANGE_B)
                        .when( AGE_COLUMN.$less( 32 ) , AGE_RANGE_C)
                        .when( AGE_COLUMN.$greater$eq( 32 ) , AGE_RANGE_D)
        );
    }

    //Metodo que recibe y regresa un dataset con la regla del ejercicio 3
    public Dataset<Row> rankNationalityPosition(Dataset<Row> input){
        WindowSpec w = Window
                .partitionBy(NATIONALITY_COLUMN,TEAM_POSITION_COLUMN)
                .orderBy(OVERALL_COLUMN.desc());
        return input.withColumn(RANK_NATIONALITY_POSITION,
                row_number().over(w)
        );
    }

    //Metodo que recibe y regresa un dataset con la regla del ejercicio 4
    public Dataset<Row> potentialOverall (Dataset<Row> input){
        return input.withColumn(POTENTIAL_VS_OVERALL_COL,
                col( POTENTIAL ).divide( col( OVERALL ) )
        );
    }

    //Metodo que recibe y regresa un dataset con la regla del ejercicio 5
    public Dataset<Row> filterColumn(Dataset<Row> input){
        return input.filter(input.col( RANK_NATIONALITY_POSITION ).$less(3)
                .or( col( AGE_RANGE_COl ).isin( AGE_RANGE_B, AGE_RANGE_C ).and( col(POTENTIAL_VS_OVERALL_COL ).$greater( 1.15 ) ) )
                .or( col( AGE_RANGE_COl ).isin( AGE_RANGE_A ).and( col(POTENTIAL_VS_OVERALL_COL ).$greater( 1.25 ) ) )
                .or( col( AGE_RANGE_COl ).isin( AGE_RANGE_D ).and( col(RANK_NATIONALITY_POSITION ).$less( 5 ) ) )
        );
    }
}
