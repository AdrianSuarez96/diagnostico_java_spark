package minsait.ttaa.datio.common;

import org.apache.spark.sql.Column;

public final class Common {

    public static final String SPARK_MODE = "local[*]";
    public static final String HEADER = "header";
    public static final String INFER_SCHEMA = "inferSchema";
    public static final String INPUT_PATH = "src/test/resources/data/players_21.csv";
    public static final String OUTPUT_PATH = "src/test/resources/data/output";

    /*Columnas del ejercicio 1*/
    public  static final Column[] OUTPUT_COLS= new Column[]{
            new Column("short_name"),
            new Column("long_name"),
            new Column("age"),
            new Column("height_cm"),
            new Column("weight_kg"),
            new Column("nationality"),
            new Column("club_name"),
            new Column("overall"),
            new Column("potential"),
            new Column("team_position")
    };
    /*Columnas del ejercicio 2*/
    public static final Column AGE_COLUMN = new Column("age");
    public static final String AGE_RANGE_COl = "age_range";

    public static final String AGE_RANGE_A = "A";
    public static final String AGE_RANGE_B = "B";
    public static final String AGE_RANGE_C = "C";
    public static final String AGE_RANGE_D = "D";

    /*Columnas del ejercicio 3*/
    public static final String RANK_NATIONALITY_POSITION = "rank_by_nationality_position";

    public static final Column NATIONALITY_COLUMN = new Column("nationality");
    public static final Column TEAM_POSITION_COLUMN = new Column("team_position");
    public static final Column OVERALL_COLUMN = new Column("overall");
    /*Columnas del ejercicio 1*/
    public static final String POTENTIAL_VS_OVERALL_COL = "potential_vs_overall";

    public static final String POTENTIAL = "potential";
    public static final String OVERALL = "overall";

}
