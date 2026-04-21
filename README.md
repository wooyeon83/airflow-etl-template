# airflow-etl-template

AWS MWAA(Managed Workflows for Apache Airflow) 기반 ETL DAG 보일러플레이트입니다.
신규 DAG를 빠르게 작성할 수 있도록 자주 쓰는 패턴(SFTP→S3, S3→Glue, Athena 검증→RDS 프로시저)을 템플릿으로 제공합니다.

---

## Airflow / MWAA 환경

| 항목 | 값 |
| --- | --- |
| MWAA 버전 | `v2.10.4` (Python 3.11) |
| Airflow Providers | Amazon 8.28.0, SFTP 4.11.0, Common-SQL 1.16.0 등 |
| 배포 대상 | S3 버킷 `s3://{env}-myorg-mwaa/` |

MWAA는 매 환경(dev/prd)마다 분리된 S3 DAG 버킷을 바라봅니다. 이 레포의 `dags/`, `requirements/`, `plugins/` 디렉터리가
각각 그 버킷의 동일 경로로 동기화됩니다.

---

## 디렉터리 구조

```
airflow-etl-template/
├── dags/
│   ├── constraints/
│   │   └── constraints-3.11.txt          # requirements 설치용 Airflow 2.10 constraints
│   ├── dependencies/
│   │   ├── hooks/                        # 공통 Hook (RDS / SFTP / Athena)
│   │   ├── operators/                    # 공통 Custom Operator
│   │   └── utils/                        # 공통 Util (date / emr_serverless)
│   ├── example_sftp_to_s3_dag.py         # [Extract] SFTP → S3 적재 템플릿
│   ├── example_glue_ingest_dag.py        # [Load]    S3 센서 → Glue Job 실행 템플릿
│   └── example_rds_procedure_dag.py      # [Transform] Athena 검증 → RDS 프로시저 호출 템플릿
├── plugins/                              # MWAA plugins.zip 대상 wheel/패키지
├── requirements/
│   └── requirements.txt
├── .gitlab-ci.yml                        # dev/prd 브랜치 → S3 sync
├── .gitignore
└── README.md
```

---

## DAG 네이밍 컨벤션

`AF{PERIOD}{DOMAIN}_{SEQ}_{SUBJECT}_DAG`

| 구분 | 값 | 설명 |
| --- | --- | --- |
| PERIOD | `E`/`M`/`W`/`Z` | Daily / Monthly / Weekly / OnDemand |
| DOMAIN | `C`/`B` | Common / Business |
| SEQ    | `A000`~ | 주제영역 번호 |

예시: `AFEC_A001_EXT_SFTP_TO_S3_DAG` (Daily · Common · 001번 · Extract)

---

## Airflow Variables

DAG는 하드코딩 대신 Airflow Variable을 통해 환경값을 주입받습니다. 변수 관리 DAG 또는 MWAA Environment에서
다음을 사전 등록해야 합니다.

| Variable Key | 예시 값 | 설명 |
| --- | --- | --- |
| `base_ymd` | `20260420` | 기준일자 (D-1) |
| `base_ym`  | `202604`   | 기준년월 (M-1) |
| `s3_bucket_nm_raw` | `dev-myorg-datalake-raw` | RAW 레이어 버킷 |
| `s3_bucket_nm_ext` | `dev-myorg-datalake-ext` | 외부연계 버킷 |
| `s3_bucket_nm_glue` | `dev-myorg-glue-asset` | Glue Job 스크립트/코드테이블 |
| `s3_bucket_nm_athena_result` | `dev-myorg-athena-result` | Athena 쿼리 결과 |
| `ext_db_nm` | `dev_myorg_ext_db` | 외부연계 Glue DB |
| `sec_manager_core_rds` | `myorg/core/rds` | Secrets Manager ARN/ID |
| `ext_sftp_authorization` | `{"sftp_host":"...","sftp_port":22,"sftp_username":"...","sftp_password":"..."}` | SFTP 접속정보 (JSON) |

---

## 로컬 개발

1. Python 3.11 가상환경 생성
2. `dags/constraints/constraints-3.11.txt` 다운로드:
   ```bash
   curl -o dags/constraints/constraints-3.11.txt \
     https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt
   ```
3. `requirements/requirements.txt` 상위 세 줄(MWAA 전용 옵션) 임시 주석 처리 후 설치:
   ```bash
   pip install -r requirements/requirements.txt --constraint dags/constraints/constraints-3.11.txt
   ```
4. `AIRFLOW_HOME=$(pwd)/.airflow` 로 지정 후 `airflow standalone` 실행
5. `dags/` 를 DAG 폴더로 연결 (`export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags`)

---

## 배포 (CI/CD)

`.gitlab-ci.yml` 은 `dev` / `prd` 브랜치 푸시 시 IAM Role Assume 후 S3로 sync 합니다.

- `dev` 브랜치 → `s3://dev-myorg-mwaa/`
- `prd` 브랜치 → `s3://prd-myorg-mwaa/`

CI 변수로 `DEV_IAMROLE`, `PRD_IAMROLE` (Assume 대상 Role ARN)을 등록해 사용합니다.

---

## 새 DAG 작성 체크리스트

- [ ] `dags/example_*_dag.py` 중 유스케이스가 가장 가까운 파일을 복제
- [ ] `dag_id`, `tags`, `description` 수정
- [ ] `start_date` / `schedule` 조정 (KST = `pendulum.timezone('Asia/Seoul')`)
- [ ] 하드코딩된 버킷/DB 이름은 `Variable.get(...)` 으로 치환
- [ ] 동적 값(`base_ymd` 등)은 `Variable.get()` 대신 `{{ var.value.key }}` Jinja 템플릿 사용
- [ ] 비밀정보는 AWS Secrets Manager / Airflow Variable 로만 참조
- [ ] 실패 알림/재시도 정책(`retries`, `retry_delay`) 설정
