import requests
from datetime import datetime, timezone, timedelta
from pyspark import TaskContext
from pyspark.sql import functions as F
import os
from .slack import send_slack_alert


DATABASE = "test"
INFLUX_URL = (
    f"http://core-influxdb3-core.influxdb.svc.cluster.local:8181/api/v3/write_lp"
    f"?db={DATABASE}&precision=nanosecond"
)

influx_token = os.environ.get("INFLUX_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {influx_token}",
    "Content-Type": "text/plain"
}

def send_raw_to_influxdb(batch_df, batch_id):
    KST = timezone(timedelta(hours=9))
    now_dt = datetime.now(KST)
    current_ns = int(now_dt.timestamp() * 1000000000)
    current_spark_time = now_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    ctx = TaskContext.get()
    partition_idx = ctx.partitionId() if ctx else 0

    def process_partition(iterator):
        lp_lines = []
        for i, row in enumerate(iterator):
            unique_timestamp = current_ns + (partition_idx * 10000) + i
            line_protocol = (
                f'raw,Symbol={row.Symbol},Partition={row.Partition} '
                f'Trade_type="{row.Trade_type}",Price={row.Price},Volume={row.Volume},'
                f'Trade_Time="{row.Trade_Time}",Timestamp_Web="{row.Timestamp_Web}",Spark_Time="{current_spark_time}",'
                f'Hash_Web="{row.Hash_Web}",Hash_Spark="{row.Hash_Spark}",Seq={row.Seq},Offset={row.Offset} '
                f'{unique_timestamp}'
            )
            lp_lines.append(line_protocol)

        if lp_lines:
            payload = "\n".join(lp_lines)
            try:
                response = requests.post(INFLUX_URL, headers=HEADERS, data=payload.encode('utf-8'), timeout=10)
                if response.status_code != 204:
                    err_msg = f"Status Code: {response.status_code}, Response: {response.text}"                    
                    # Raw 데이터 전송 실패 시 슬랙 알림
                    send_slack_alert(
                        title="⚠️ InfluxDB Raw 데이터 적재 실패",
                        summary=f"Batch {batch_id} (Partition {partition_idx}) 전송 중 오류 발생",
                        details=err_msg,
                        level="warning"
                    )
                    
                    print(f"Error sending data: {response.text}")
            except Exception as e:
                err_msg = str(e)
                
                # 네트워크 통신 등 예외 발생 시 슬랙 알림
                send_slack_alert(
                    title="🚨 InfluxDB Raw 연결 에러",
                    summary=f"Batch {batch_id} (Partition {partition_idx}) 통신 중 예외 발생",
                    details=err_msg,
                    level="danger"
                )
                print(f"Exception during InfluxDB request: {err_msg}")
                
    batch_df.rdd.foreachPartition(process_partition)
    print(f"[Raw] Batch {batch_id} 전송 완료")


def process_batch_5sec(batch_df, batch_id):
    agg_df = batch_df \
        .filter(F.col("Trade_Time") >= F.expr("current_timestamp() - interval 5 seconds")) \
        .groupBy("Symbol") \
        .agg(
            F.min_by("Price", "Trade_Time").alias("Open"),
            F.max_by("Price", "Trade_Time").alias("Close"),
            F.max("Price").alias("High"),
            F.min("Price").alias("Low"),
            F.sum("Volume").alias("Total_Volume"),
            F.min("Timestamp_Web").alias("Start_Time"),
            F.max("Timestamp_Web").alias("End_Time")
        )

    rows = agg_df.collect()
    if not rows:
        print(f"[5sec] Batch {batch_id} - 처리할 데이터가 없습니다.")
        return

    KST = timezone(timedelta(hours=9))
    now_dt = datetime.now(KST)
    current_ns = int(now_dt.timestamp() * 1000000000)
    current_spark_time = now_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    five_sec_ns = 5 * 1000000000
    unique_timestamp = (current_ns // five_sec_ns) * five_sec_ns - five_sec_ns

    lp_lines = []
    lp_lines_monitor = []

    for row in rows:
        line_protocol = (
            f'5sec,Symbol={row.Symbol} '
            f'Open={row.Open},High={row.High},Low={row.Low},Close={row.Close},'
            f'Total_Volume={row.Total_Volume},'
            f'Spark_Time="{current_spark_time}",'
            f'Start_Time="{row.Start_Time}",End_Time="{row.End_Time}" '
            f'{unique_timestamp}'
        )
        lp_lines.append(line_protocol)

        line_protocol_monitor = (
            f'5sec-monitor,Symbol={row.Symbol} '
            f'Open={row.Open},High={row.High},Low={row.Low},Close={row.Close},'
            f'Total_Volume={row.Total_Volume},'
            f'Spark_Time="{current_spark_time}",'
            f'Start_Time="{row.Start_Time}",End_Time="{row.End_Time}" '
            f'{unique_timestamp}'
        )
        lp_lines_monitor.append(line_protocol_monitor)

    if lp_lines:
        payload = "\n".join(lp_lines) + "\n" + "\n".join(lp_lines_monitor)
        try:
            response = requests.post(INFLUX_URL, headers=HEADERS, data=payload.encode('utf-8'), timeout=10)
            if response.status_code != 204:
                err_msg = f"Status Code: {response.status_code}, Response: {response.text}"
                
                # 5초 집계 데이터 전송 실패 시 슬랙 알림 (중요 지표이므로 danger 레벨 부여 가능)
                send_slack_alert(
                    title="⚠️ InfluxDB 5초 집계(5sec) 적재 실패",
                    summary=f"Batch {batch_id} 집계 데이터 전송 중 오류 발생",
                    details=err_msg,
                    level="danger"
                )
                print(f"집계 전송 실패: {err_msg}")

        except Exception as e:
            err_msg = str(e)            
            # 네트워크 통신 등 예외 발생 시 슬랙 알림
            send_slack_alert(
                title="🚨 InfluxDB 5초 집계(5sec) 연결 에러",
                summary=f"Batch {batch_id} 통신 중 예외 발생",
                details=err_msg,
                level="danger"
            )
            print(f"집계 데이터 전송 에러: {err_msg}")

    print(f"[5sec] Batch {batch_id} 전송 완료")