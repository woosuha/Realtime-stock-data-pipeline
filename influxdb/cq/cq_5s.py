from influxdb_client_3 import InfluxDBClient3
import pandas as pd
import requests
import os
import time
import logging
from datetime import datetime

from utils.slack import send_slack_alert

# 로그 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# InfluxDB Connection 및 데이터 추출 설정
influx_token = os.environ.get("INFLUX_TOKEN")

client = InfluxDBClient3(
    host="grpc+tcp://core-influxdb3-core.influxdb.svc.cluster.local:8181",
    token=f"{influx_token}",
    database="test"
)

DATABASE = "test"
INFLUX_URL = (
    f"http://core-influxdb3-core.influxdb.svc.cluster.local:8181/api/v3/write_lp"
    f"?db={DATABASE}&precision=nanosecond"
)

HEADERS = {
    "Authorization": f"Bearer {influx_token}",
    "Content-Type": "text/plain"
}


# 시작 알람
send_slack_alert(
    title="🚀 5초봉 데이터 파이프라인 시작",
    summary="실시간 집계 프로세스가 시작되었습니다.",
    details=(
        f"시작시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    ),
    level="info"
)


while True:
    start_time = time.time()
    
    try:
        # 1. 지금부터 10분 전 데이터 조회
        query = """
            SELECT "Price", "Symbol", "Trade_Time", "Volume"
            FROM "raw"
            WHERE to_timestamp("Trade_Time") >= now() - interval '10 minute' + interval '9 hours'
            ORDER BY "Trade_Time"
        """
        df = client.query(query=query, language="sql", mode="pandas")
        
        # 데이터가 없을 경우 에러 방지
        if df is not None and not df.empty:
            # 2. Trade_Time을 datetime 형식으로 변환
            df["Trade_Time"] = pd.to_datetime(df["Trade_Time"], format="mixed")

            # 3. pd.Grouper를 사용하여 Symbol별, 시간(5초)별로 그룹화 및 집계
            df = (
                df.groupby(["Symbol", pd.Grouper(key="Trade_Time", freq="5s")])
                .agg(
                    Open=("Price", "first"),
                    High=("Price", "max"),
                    Low=("Price", "min"),
                    Close=("Price", "last"),
                    Total_Volume=("Volume", "sum"),
                )
                .reset_index()
            )

            # 4. 이평선 동적 계산
            windows = [5, 10, 20, 60]
            for w in windows:
                df[f"MA_{w}"] = (
                    df.groupby("Symbol")["Close"]
                    .transform(lambda x: x.rolling(window=w).mean())
    )

            # 5. 컬럼명 변경 및 타임스탬프 포맷팅
            final_df = df.rename(columns={"Trade_Time": "time"})
            final_df["time"] = (final_df["time"] - pd.Timedelta(hours=9)).astype("int64") * 1000

            # 6. InfluxDB 라인 프로토콜 생성 (window 반복문 적용)
            lp_lines_monitor = []
            for row in final_df.itertuples():
                fields = [
                    f"Open={row.Open}",
                    f"High={row.High}",
                    f"Low={row.Low}",
                    f"Close={row.Close}",
                    f"Total_Volume={row.Total_Volume}"
                ]
                
                # 리스트 루프를 통해 동적으로 MA 필드 추가
                for w in windows:
                    ma_val = getattr(row, f"MA_{w}")
                    if not pd.isna(ma_val):
                        fields.append(f"MA_{w}={ma_val}")

                fields_str = ",".join(fields)

                line_protocol_monitor = (
                    f'5sec-monitor,Symbol={row.Symbol} '
                    f'{fields_str} '
                    f'{row.time}'
                )
                lp_lines_monitor.append(line_protocol_monitor)

            # 7. InfluxDB로 전송
            if lp_lines_monitor :
                payload = "\n".join(lp_lines_monitor)
                # print("--------------------[집계 DATA PAYLOAD]----------------")
                try:
                    response = requests.post(INFLUX_URL, headers=HEADERS, data=payload.encode('utf-8'), timeout=10)
                    if response.status_code != 204:
                        error_message = (f"HTTP Status: {response.status_code}\n"
                                         f"Response: {response.text[:1000]}\n"
                                         f"전송 데이터: {len(lp_lines_monitor)}건")
                        logging.info(f"집계 전송 실패: {error_message}")

                        send_slack_alert(
                            title="🚨 InfluxDB 전송 실패",
                            summary="5초봉 데이터 전송에 실패했습니다.",
                            details=error_message,
                            level="danger"
                        )

                    else:
                        logging.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S]')} 5초봉 데이터 {len(lp_lines_monitor)}건 전송 완료")
                except Exception as e:
                    logging.info(f"집계 데이터 전송 에러: {e}")

        else:
            logging.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S]')} 조회된 데이터가 없습니다.")

    except Exception as e:
        logging.info(f"실행 중 에러 발생: {e}")
        error_message = (
            f"예외 종류: {type(e).__name__}\n"
            f"내용: {str(e)}\n"
            f"발생시간: "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        send_slack_alert(
            title="🚨 5초봉 파이프라인 오류",
            summary="데이터 집계 과정에서 예외가 발생했습니다.",
            details=error_message,
            level="danger"
        )

    # 8. 5초 주기
    now = time.time()
    sleep_time = 5 - (now % 5)
    time.sleep(sleep_time)