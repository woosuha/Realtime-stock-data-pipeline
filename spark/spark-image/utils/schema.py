import requests
from .slack import send_slack_alert

def read_avro_schema_from_registry():
    SCHEMA_REGISTRY_URL = "http://apicurio-registry.kafka.svc:8080/apis/ccompat/v7"
    SCHEMA_SUBJECT = "web-producer"
    SCHEMA_VERSION = "latest"
    
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/{SCHEMA_VERSION}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            err_msg = f"Schema Registry 조회 실패: Status Code: {response.status_code}, Response: {response.text}"
            
            # 슬랙 알람 전송
            send_slack_alert(
                title="🚨 스키마 레지스트리(Apicurio) 조회 실패",
                summary=f"Subject '{SCHEMA_SUBJECT}'의 스키마를 가져오지 못했습니다.",
                details=err_msg,
                level="danger"
            )
            
            raise Exception(err_msg)
            
        return response.json()["schema"]
        
    except requests.exceptions.RequestException as e:
        err_msg = f"Schema Registry 연결 에러 발생: {e}"
        
        # 네트워크 연결 자체에 실패했을 때도 슬랙 알람 전송
        send_slack_alert(
            title="🚨 스키마 레지스트리(Apicurio) 연결 에러",
            summary="Apicurio Registry 서버와 통신 중 예외가 발생했습니다.",
            details=err_msg,
            level="danger"
        )
        
        raise Exception(err_msg)

