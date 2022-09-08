package local.bigdata.tchile.resoluciones_comerciales

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Principal {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()

    import spark.implicits._

    // PARAMETROS

    // PATHS

    //    val conformadoBasePath = ""
    //    val conformadoFullPath = s"$conformadoBasePath/year=$processYear/month=$processMonth/day=$processDay"
    val conformadoFullPath = "/data/parque_asignado/parque/b2b/scel/resolucionesComercialesPrincipalScala/mvp/"
    val sisonSolicitudPath = "/data/ventas/preventa/sison/son_solicitud_evc/raw"
    val carteraEmpresaPath = "/warehouse/staging/datalakeb2b/fil_cartera_empresa_v2"

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    val sisonSolicitud = spark.read.parquet(sisonSolicitudPath).
      withColumnRenamed("BIGDATA_CLOSE_DATE", "PERIODO").
      filter(from_unixtime(unix_timestamp($"feccreacion", "yyyyMMdd"), "yyyy") >= 2021).
      select(
        "NUMSOLICITUD",
        "CODTIPOSOLICITUD",
        "DESTIPOSOLICITUD",
        "CODSERVICIOSOLICITUD",
        "DESSERVICIOSOLICITUD",
        "CODMOTIVO",
        "DESMOTIVO",
        "CODFACTUARCION",
        "DESFACTURACION",
        "FECCREACION",
        "NUMRESPONSABLE",
        "NUMOPORTUNIDAD",
        "RUTCLIENTE",
        "NOMCLIENTE",
        "DESSEGMENTO",
        "NUMCONTACTO",
        "RUNCONTACTO",
        "APEPATERNOCONTACTO",
        "APEMATERNOCONTACTO",
        "NOMBRECONTACTO",
        "TELCONTACTO",
        "MAICONTACTO",
        "DESCARGOCONTACTO",
        "NUMCONTRATO",
        "INDCONTRATO",
        "INDCLAUSULAANTI",
        "FECINICIOCTR",
        "NUMCUOTA",
        "CANCUOTAS",
        "GLORESUMEN",
        "FECESTADO",
        "CODESTADO",
        "DESESTADO",
        "FECPLAZORESOL",
        "INDMASIVO",
        "DESAREAINGRESADORA",
        "FOLIOFACTURA",
        "MTOFACTURA",
        "FECFACTURA",
        "NUMRECLAMO",
        "FECRECLAMO",
        "MTOTOTALNC",
        "MTOTOTALAPROBADO",
        "CODTIPOFACTURADOR",
        "DESTIPOFACTURADOR",
        "DESSEGMENTOON",
        "PERIODO"
      )

    val carteraEmpresa = spark.read.orc(carteraEmpresaPath).
      withColumn("RUTCLIENTE",  substring(concat(lit("0000000000"), $"RUT_HIJA"), -10, 10)).
      withColumnRenamed("NOMBRE_SUBGERENTE_AM", "NOMBRE_GERENTE_AREA").
      withColumnRenamed("EMAIL_SUBGERENTE_AM", "EMAIL_GERENTE_AREA").
      withColumnRenamed("NOMBRE_JEFE_COMERCIAL_AM", "NOMBRE_JEFE_COMERCIAL").
      select(
        "RUTCLIENTE",
        "NOMBRE_AM",
        "EMAIL_AM",
        "NOMBRE_GERENTE_AREA",
        "EMAIL_GERENTE_AREA",
        "NOMBRE_GERENTE_COMERCIAL",
        "EMAIL_GERENTE_COMERCIAL",
        "NOMBRE_JEFE_COMERCIAL",
        "NOMBRE_SERVICE_MANAGER"
      )

    val conformado = sisonSolicitud.
      join(carteraEmpresa, Seq("RUTCLIENTE"), "left").
      withColumn("FECHA_CARGA", current_date()).
      select(
        "NUMSOLICITUD",
        "CODTIPOSOLICITUD",
        "DESTIPOSOLICITUD",
        "CODSERVICIOSOLICITUD",
        "DESSERVICIOSOLICITUD",
        "CODMOTIVO",
        "DESMOTIVO",
        "CODFACTUARCION",
        "DESFACTURACION",
        "FECCREACION",
        "NUMRESPONSABLE",
        "NUMOPORTUNIDAD",
        "RUTCLIENTE",
        "NOMCLIENTE",
        "DESSEGMENTO",
        "NUMCONTACTO",
        "RUNCONTACTO",
        "APEPATERNOCONTACTO",
        "APEMATERNOCONTACTO",
        "NOMBRECONTACTO",
        "TELCONTACTO",
        "MAICONTACTO",
        "DESCARGOCONTACTO",
        "NOMBRE_AM",
        "EMAIL_AM",
        "NOMBRE_GERENTE_AREA",
        "EMAIL_GERENTE_AREA",
        "NOMBRE_GERENTE_COMERCIAL",
        "EMAIL_GERENTE_COMERCIAL",
        "NUMCONTRATO",
        "INDCONTRATO",
        "INDCLAUSULAANTI",
        "FECINICIOCTR",
        "NUMCUOTA",
        "CANCUOTAS",
        "GLORESUMEN",
        "FECESTADO",
        "CODESTADO",
        "DESESTADO",
        "FECPLAZORESOL",
        "INDMASIVO",
        "NOMBRE_JEFE_COMERCIAL",
        "DESAREAINGRESADORA",
        "FOLIOFACTURA",
        "MTOFACTURA",
        "FECFACTURA",
        "NUMRECLAMO",
        "FECRECLAMO",
        "MTOTOTALNC",
        "MTOTOTALAPROBADO",
        "NOMBRE_SERVICE_MANAGER",
        "CODTIPOFACTURADOR",
        "DESTIPOFACTURADOR",
        "DESSEGMENTOON",
        "PERIODO",
        "FECHA_CARGA"
      )

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