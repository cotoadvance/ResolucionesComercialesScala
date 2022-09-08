package local.bigdata.tchile.resoluciones_comerciales

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

object Glosario {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()

    // PARAMETROS

    // PATHS

    //val conformadoBasePath = "/modelos/visualizacion/agrupado/b2b_sem_agr_parque_movil"
    //val conformadoFullPath = s"$conformadoBasePath/year=$processYear/month=$processMonth/day=$processDay"
    val conformadoFullPath = "/data/parque_asignado/parque/b2b/scel/resolucionesComercialesGlosarioScala/mvp/"
    val sisonSolicitudGlosaPath = "/data/ventas/preventa/sison/son_solicitud_evc_glosadatos/raw"

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    val conformado = spark.read.parquet(sisonSolicitudGlosaPath).
      withColumn("FECHA_CARGA", current_date()).
      select("NUMSOLICITUD", "CODCARPETA", "GLOSA", "FECHA_CARGA")

    // ---------------------------------------------------------------
    // FIN PROCESO
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HDFS
    // ---------------------------------------------------------------

    conformado.repartition(1).write.mode("overwrite").parquet(conformadoFullPath)

    // ---------------------------------------------------------------
    // FIN ESCRITURA HDFS
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HIVE
    // ---------------------------------------------------------------

    //    spark.sql(s"DROP TABLE IF EXISTS $hiveSchema.$table")
    //
    //    spark.sql(
    //      s"""CREATE EXTERNAL TABLE $hiveSchema.$table(
    //         |AGNO string,
    //         |MES string,
    //         |SEGMENTO string,
    //         |CANT_LINEAS long,
    //         |CANT_RUT long,
    //         |FECHA_CARGA date
    //         |)
    //         |PARTITIONED BY
    //         |(year string, month string, day string)
    //         |STORED AS PARQUET
    //         |LOCATION '$conformadoBasePath'""".stripMargin)
    //
    //    spark.sql(s"MSCK REPAIR TABLE $hiveSchema.$table")

    // ---------------------------------------------------------------
    // FIN ESCRITURA HIVE
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA EXADATA
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // FIN ESCRITURA EXADATA
    // ---------------------------------------------------------------

    spark.close()
  }
}