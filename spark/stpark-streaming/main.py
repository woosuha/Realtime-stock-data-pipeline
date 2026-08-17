from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
import logging

from utils.schema import read_avro_schema_from_registry
from utils.metrics import init_prometheus, KafkaOffsetListener
from utils.influx_writer import send_raw_to_influxdb, process_batch_5sec
from utils.slack import send_slack_alert

def main():
    # 1. 앱 시작 알림
    send_slack_alert(
        title="🚀 Spark 스트리밍 앱 시작",
        summary="SparkApplication 드라이버가 구동되었습니다.",
        details="애플리케이션 초기화 및 스트리밍 쿼리 준비 중...",
        level="info"
    )
    try :

        # 1. Prometheus 메트릭 서버 시작
        init_prometheus()

        # 2. Spark 세션 생성 및 리스너 등록
        spark = SparkSession.builder \
            .appName("spark-streaming") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        spark.streams.addListener(KafkaOffsetListener())

        # 3. 스키마 로드
        schema = read_avro_schema_from_registry()

        # 4. Kafka 스트림 읽기
        table = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "my-cluster-kafka-bootstrap.kafka.svc:9092") \
            .option("subscribe", "stock_data") \
            .load()

        avro_data = table.select(
            from_avro("value", schema).alias("parsed_value"),
            F.col("partition"),
            F.col("offset")
        )

        # 데이터 전처리 및 변환 (Transformations)
        exploded_data = avro_data.select(
            F.explode("parsed_value.data").alias("exploded"),
            F.col("parsed_value.timestamp_web")[0].alias("timestamp_web"),
            F.col("partition").alias("Partition"), 
            F.col("offset").alias("Offset")
        )

        final_data = exploded_data.select(
            F.explode("exploded").alias("exploded_item"),
            "timestamp_web", "Offset", "Partition"
        )

        df = final_data.select(
            F.col("exploded_item")["c"].alias("Trade_type"),
            F.col("exploded_item")["p"].cast("double").alias("Price"),
            F.col("exploded_item")["s"].alias("Symbol"),
            F.col("exploded_item")["v"].cast("double").alias("Volume"),
            F.col("exploded_item")["t"].alias("Trade_Timestamp"),
            F.from_utc_timestamp(
                F.timestamp_millis(F.col("exploded_item").getField("t")), 
                "Asia/Seoul"
            ).alias("Trade_Time"),
            F.col("exploded_item")["hash"].alias("Hash_Web"),
            F.col("exploded_item")["seq"].alias("Seq"),
            F.col("timestamp_web").alias("Timestamp_Web"),
            F.col("Offset"),
            F.col("Partition")
        )

        df = df.withColumn("c_json", F.to_json(F.col("Trade_type")))

        df = df.withColumn("Hash_Spark", F.sha2(
            F.concat_ws(
                "|",
                F.col("Symbol"),
                F.col("Trade_Timestamp"),
                F.format_string("%.10f", F.col("Price")),
                F.format_string("%.10f", F.col("Volume")),
                F.coalesce(F.col("c_json"), F.lit("null"))
            ), 256))

        df = df.drop("Trade_Timestamp", "c_json")

        # 5. 스트리밍 쿼리 실행 (Raw & 5sec Aggregation)
        query_raw = df.writeStream \
            .queryName("raw") \
            .foreachBatch(send_raw_to_influxdb) \
            .outputMode("append") \
            .option("checkpointLocation", "gs://stock-spark-applications/check/exam-hash-raw/raw") \
            .start()

        query_agg_5sec = df.writeStream \
            .queryName("agg_5sec") \
            .foreachBatch(process_batch_5sec) \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", "gs://stock-spark-applications/check/exam-hash-5sec/agg") \
            .start()

        spark.streams.awaitAnyTermination()

    except Exception as e:
        err_msg = f"Exception Type: {type(e).__name__}\nDetails: {str(e)}"
        logging.error(f"애플리케이션 치명적 예외 발생: {err_msg}")
        
        # 슬랙 알림 발송
        send_slack_alert(
            title="🔥 Spark 스트리밍 앱 비정상 종료 발생",
            summary="애플리케이션이 예외로 인해 비정상 종료됩니다.",
            details=err_msg,
            level="danger"
        )
        

if __name__ == "__main__":
    main()