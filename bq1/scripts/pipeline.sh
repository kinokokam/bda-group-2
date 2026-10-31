#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
cd "$here/../.."
if [ -f .env ]; then set -a; . ./.env; set +a; fi
. "$here/config.sh"

export AWS_PROFILE="${AWS_PROFILE:-bda}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-ap-southeast-1}"

BUCKET="${S3_BUCKET_NAME:-aml-bq1}"
ARTIFACTS=build
MODEL_PREFIX=model/

# Gitignored, so absent on a fresh clone; predemo and replay write here.
mkdir -p "$ARTIFACTS"

bootstrap() {
  local arn
  arn=$(aws kafka list-clusters-v2 --cluster-name-filter "$CLUSTER" \
    --query 'ClusterInfoList[?State==`ACTIVE`]|[0].ClusterArn' --output text)
  if [ -z "$arn" ] || [ "$arn" = "None" ]; then
    echo "no ACTIVE MSK cluster named $CLUSTER" >&2
    exit 1
  fi
  aws kafka get-bootstrap-brokers --cluster-arn "$arn" \
    --query BootstrapBrokerStringSaslIam --output text
}

set_lambda_bootstrap() {
  local servers current
  servers=$(bootstrap)
  current=$(aws lambda get-function-configuration --function-name "$LAMBDA" \
    --query 'Environment.Variables' --output json)
  aws lambda update-function-configuration --function-name "$LAMBDA" \
    --environment "$(printf '%s' "$current" | python -c "
import json, sys
variables = json.load(sys.stdin)
variables['BOOTSTRAP'] = sys.argv[1]
json.dump({'Variables': variables}, sys.stdout)
" "$servers")" >/dev/null
  aws lambda wait function-updated --function-name "$LAMBDA"
}

status() {
  aws kinesisanalyticsv2 describe-application --application-name "$APP" \
    --query 'ApplicationDetail.ApplicationStatus' --output text
}

set_props() {
  local version groups
  version=$(aws kinesisanalyticsv2 describe-application --application-name "$APP" \
    --query 'ApplicationDetail.ApplicationVersionId' --output text)
  groups=$(aws kinesisanalyticsv2 describe-application --application-name "$APP" \
    --query 'ApplicationDetail.ApplicationConfigurationDescription.EnvironmentPropertyDescriptions.PropertyGroupDescriptions' \
    --output json)
  aws kinesisanalyticsv2 update-application \
    --application-name "$APP" \
    --current-application-version-id "$version" \
    --application-configuration-update \
    "$(printf '%s' "$groups" | python "$here/property_update.py" "$@")" >/dev/null
}

configure_kafka_phase() {
  local run_id="$1" gold_from="$2"
  set_lambda_bootstrap
  create_topics "transactions-$run_id" "alerts-$run_id" "rejected-$run_id"
  set_props \
    "bootstrap.servers=$(bootstrap)" \
    "source.mode=kafka" \
    "source.topic=transactions-$run_id" \
    "kafka.sinks=true" \
    "score.enabled=true" \
    "gold.mode=$GOLD_MODE" \
    "alerts.topic=alerts-$run_id" \
    "rejected.topic=rejected-$run_id" \
    "run.id=$run_id" \
    "glue.database=$GLUE_DATABASE" \
    "gold.from=$gold_from"
}

configure_file_phase() {
  local run_id="$1" from="$2" to="$3" gold_from="$4" scoring="${5:-true}"
  set_props \
    "bootstrap.servers=pending" \
    "kafka.sinks=false" \
    "source.mode=files" \
    "source.from=$from" \
    "source.to=$to" \
    "score.enabled=$scoring" \
    "gold.mode=$GOLD_MODE" \
    "alerts.topic=alerts-$run_id" \
    "rejected.topic=rejected-$run_id" \
    "run.id=$run_id" \
    "glue.database=$GLUE_DATABASE" \
    "gold.from=$gold_from"
}

records_out() {
  aws cloudwatch get-metric-statistics \
    --namespace AWS/KinesisAnalytics --metric-name numRecordsOut \
    --dimensions Name=Application,Value="$APP" \
    --start-time "$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%S)" \
    --end-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
    --period 60 --statistics Maximum \
    --query 'max(Datapoints[].Maximum)' --output text 2>/dev/null
}

await_drained() {
  local previous="" current steady=0
  while [ "$steady" -lt 3 ]; do
    sleep 120
    current=$(records_out)
    case "$current" in ''|None) current=0 ;; esac
    if [ "$current" = "$previous" ]; then
      steady=$((steady + 1))
    else
      steady=0
      echo "processed $current records so far"
    fi
    previous="$current"
  done
  echo "drained at $previous records"
}

snapshot() {
  sleep "$DRAIN"
  aws kinesisanalyticsv2 delete-application-snapshot \
    --application-name "$APP" --snapshot-name "$1" \
    --snapshot-creation-timestamp 0 >/dev/null 2>&1 || true
  aws kinesisanalyticsv2 create-application-snapshot \
    --application-name "$APP" --snapshot-name "$1" >/dev/null
}

start_app() {
  aws kinesisanalyticsv2 start-application --application-name "$APP" \
    --run-configuration \
    "{\"FlinkRunConfiguration\":{\"AllowNonRestoredState\":true},\"ApplicationRestoreConfiguration\":$1}" \
    >/dev/null
  until [ "$(status)" = "RUNNING" ]; do sleep 10; done
}

stop_app() {
  aws kinesisanalyticsv2 stop-application --application-name "$APP" >/dev/null
  until [ "$(status)" = "READY" ]; do sleep 10; done
}

create_topics() {
  local list
  list=$(printf '"%s",' "$@")
  aws lambda invoke --function-name "$LAMBDA" \
    --payload "{\"action\":\"create-topics\",\"topics\":[${list%,}]}" \
    "$ARTIFACTS/lambda-out.json" >/dev/null
}

replay() {
  aws lambda invoke --function-name "$LAMBDA" \
    --cli-read-timeout 900 --cli-connect-timeout 20 \
    --payload "{\"start\":\"$1\",\"end\":\"$2\",\"speed\":$3,\"topic\":\"$4\"}" \
    "$ARTIFACTS/lambda-out.json" >/dev/null
  cat "$ARTIFACTS/lambda-out.json"
  echo
}

chunks() {
  python - "$1" "$2" "$CHUNK_HOURS" <<'PY'
import sys
from datetime import datetime, timedelta

FORMAT = "%Y-%m-%dT%H:%M"
cursor = datetime.strptime(sys.argv[1], FORMAT)
end = datetime.strptime(sys.argv[2], FORMAT)
step = timedelta(hours=int(sys.argv[3]))
while cursor < end:
    following = min(cursor + step, end)
    print(f"{cursor.strftime(FORMAT)} {following.strftime(FORMAT)}")
    cursor = following
PY
}

replay_window() {
  local start="$1" end="$2" speed="$3" topic="$4" from to
  while read -r from to; do
    echo "replaying $from -> $to"
    replay "$from" "$to" "$speed" "$topic"
  done < <(chunks "$start" "$end")
}

case "${1:-usage}" in

  build)
    mkdir -p "$ARTIFACTS"

    if [ ! -d "$ARTIFACTS/site-packages" ]; then
      MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD/$ARTIFACTS/site-packages:/out" python:3.11-slim-bookworm \
        pip install --quiet --only-binary=:all: --platform manylinux2014_x86_64 \
        --target /out "xgboost==2.1.4" "scikit-learn==1.6.1" joblib "numpy==1.24.4" scipy
    fi

    rm -rf "$ARTIFACTS"/site-packages/cloudpickle \
           "$ARTIFACTS"/site-packages/cloudpickle-*.dist-info

    mkdir -p "$ARTIFACTS/model"
    for artifact in xgboost_aml_model.json xgboost_features.json calibrator.pkl \
                    category_maps.json config.json; do
      [ -f "$ARTIFACTS/model/$artifact" ] || \
        aws s3api get-object --bucket "$BUCKET" --key "$MODEL_PREFIX$artifact" \
          "$ARTIFACTS/model/$artifact" >/dev/null
    done

    if [ ! -f "$ARTIFACTS/lib/aml-bq1-kafka-iam.jar" ] || \
       [ bq1/flink/jar/pom.xml -nt "$ARTIFACTS/lib/aml-bq1-kafka-iam.jar" ]; then
      mkdir -p "$ARTIFACTS/lib" "$ARTIFACTS/m2"
      MSYS_NO_PATHCONV=1 docker run --rm \
        -v "$PWD/bq1/flink/jar:/src" -v "$PWD/$ARTIFACTS/m2:/root/.m2" -w /src \
        maven:3.9-eclipse-temurin-11 mvn -q -B clean package
      cp bq1/flink/jar/target/aml-bq1-kafka-iam.jar "$ARTIFACTS/lib/"
    fi

    cp bq1/flink/features.py "$ARTIFACTS/site-packages/features.py"
    python bq1/scripts/zip_app.py \
      --out "$ARTIFACTS/flink.zip" \
      --main bq1/flink/main.py \
      --deps "$ARTIFACTS/site-packages" \
      --jar "$ARTIFACTS/lib/aml-bq1-kafka-iam.jar" \
      --py-files bq1/flink/features.py \
      --data "$ARTIFACTS/model/xgboost_aml_model.json" \
             "$ARTIFACTS/model/xgboost_features.json" \
             "$ARTIFACTS/model/calibrator.pkl" \
             "$ARTIFACTS/model/category_maps.json" \
             "$ARTIFACTS/model/config.json"

    rm -rf "$ARTIFACTS/replay" && mkdir -p "$ARTIFACTS/replay"
    python -m pip install --quiet --target "$ARTIFACTS/replay" \
      "kafka-python==3.0.11" "aws-msk-iam-sasl-signer-python==1.0.2"
    rm -rf "$ARTIFACTS/replay"/boto3 "$ARTIFACTS/replay"/boto3-*.dist-info \
           "$ARTIFACTS/replay"/botocore "$ARTIFACTS/replay"/botocore-*.dist-info \
           "$ARTIFACTS/replay"/s3transfer "$ARTIFACTS/replay"/s3transfer-*.dist-info
    cp bq1/lambdas/replay/handler.py "$ARTIFACTS/replay/handler.py"
    cp bq1/flink/features.py "$ARTIFACTS/replay/features.py"
    (cd "$ARTIFACTS/replay" && python -c "
import pathlib, zipfile
with zipfile.ZipFile('../replay.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(pathlib.Path('.').rglob('*')):
        if path.is_file():
            archive.write(path, path.as_posix())
")

    aws s3 sync "$ARTIFACTS" "s3://$BUCKET/artifacts/" \
      --exclude '*' --include 'flink.zip' --include 'replay.zip' --only-show-errors
    aws s3 ls "s3://$BUCKET/artifacts/"
    ;;

  provision)
    cd infra
    [ -f terraform.tfvars ] || cp terraform.tfvars.example terraform.tfvars
    terraform init -input=false
    terraform apply -input=false -auto-approve \
      -var deploy_lambda=true -var deploy_flink=true -var deploy_glue_endpoint=true
    echo
    echo "--- paste into .env ---"
    terraform output -raw env_file_block
    url=$(terraform output -raw dashboard_url)
    echo
    echo "dashboard: ${url:-not deployed (set dashboard_allowed_cidr in infra/terraform.tfvars)}"
    ;;

  dashboard-build)
    rm -f "$ARTIFACTS/dashboard.zip"
    python - "$ARTIFACTS/dashboard.zip" <<'PY'
import pathlib, sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(pathlib.Path("frontend").rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            archive.write(path, path.as_posix())
PY
    aws s3 cp "$ARTIFACTS/dashboard.zip" "s3://$BUCKET/artifacts/dashboard.zip" --only-show-errors
    echo "uploaded s3://$BUCKET/artifacts/dashboard.zip"
    ;;

  dashboard-redeploy)
    "$0" dashboard-build
    (cd infra && terraform apply -input=false -auto-approve -target aws_instance.dashboard \
      -var deploy_lambda=true -var deploy_flink=true -var deploy_glue_endpoint=true)
    echo "dashboard: $(cd infra && terraform output -raw dashboard_url) (allow a few minutes to install)"
    ;;

  deploy)
    aws lambda update-function-code --function-name "$LAMBDA" \
      --s3-bucket "$BUCKET" --s3-key artifacts/replay.zip \
      --query 'LastUpdateStatus' --output text
    aws lambda wait function-updated --function-name "$LAMBDA"
    version=$(aws kinesisanalyticsv2 describe-application --application-name "$APP" \
      --query 'ApplicationDetail.ApplicationVersionId' --output text)
    aws kinesisanalyticsv2 update-application --application-name "$APP" \
      --current-application-version-id "$version" \
      --application-configuration-update \
      "{\"ApplicationCodeConfigurationUpdate\":{\"CodeContentUpdate\":{\"S3ContentLocationUpdate\":{\"BucketARNUpdate\":\"arn:aws:s3:::$BUCKET\",\"FileKeyUpdate\":\"artifacts/flink.zip\"}}}}" \
      --query 'ApplicationDetail.[ApplicationStatus,ApplicationVersionId]' --output text
    ;;

  smoke)
    configure_kafka_phase smoke ""
    start_app '{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}'
    replay "$START" "2022-09-10T11:05" 0 transactions-smoke
    echo "smoke replay done; check gold and the logs before going further"
    ;;

  warmup)
    configure_file_phase warmup "$WARMUP_START" "$BACKFILL_START" "$GOLD_FROM" false
    start_app '{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}'
    await_drained
    snapshot "$SNAPSHOT_WARMUP"
    stop_app
    echo "$SNAPSHOT_WARMUP"
    ;;

  warmup-kafka)
    configure_kafka_phase warmup "$GOLD_FROM"
    start_app '{"ApplicationRestoreType":"SKIP_RESTORE_FROM_SNAPSHOT"}'
    replay_window "$WARMUP_START" "$BACKFILL_START" 0 transactions-warmup
    snapshot "$SNAPSHOT_WARMUP"
    stop_app
    echo "$SNAPSHOT_WARMUP"
    ;;

  backfill-kafka)
    configure_kafka_phase backfill "$GOLD_FROM"
    start_app "{\"ApplicationRestoreType\":\"RESTORE_FROM_CUSTOM_SNAPSHOT\",\"SnapshotName\":\"$SNAPSHOT_WARMUP\"}"
    replay_window "$BACKFILL_START" "$START" 0 transactions-backfill
    snapshot "$SNAPSHOT_DEMO"
    stop_app
    echo "$SNAPSHOT_DEMO"
    ;;

  backfill)
    configure_file_phase backfill "$BACKFILL_START" "${BACKFILL_END:-$START}" "$GOLD_FROM"
    start_app "{\"ApplicationRestoreType\":\"RESTORE_FROM_CUSTOM_SNAPSHOT\",\"SnapshotName\":\"$SNAPSHOT_WARMUP\"}"
    await_drained
    snapshot "$SNAPSHOT_DEMO"
    stop_app
    echo "$SNAPSHOT_DEMO"
    ;;

  run)
    run_id=$(date +%Y%m%d-%H%M)
    configure_kafka_phase "$run_id" "$GOLD_FROM"
    start_app "{\"ApplicationRestoreType\":\"RESTORE_FROM_CUSTOM_SNAPSHOT\",\"SnapshotName\":\"$SNAPSHOT_DEMO\"}"
    replay "$START" "$END" "$SPEED" "transactions-$run_id"
    echo "transactions-$run_id"
    ;;

  setup)
    "$0" build
    "$0" provision
    "$0" deploy
    "$0" warmup
    "$0" backfill
    echo "setup done; $SNAPSHOT_DEMO holds the account state and survives teardown"
    echo "demo now with 'predemo' then 'demo', or run 'teardown' and come back later"
    ;;

  predemo)
    "$0" dashboard-build
    "$0" provision
    run_id=$(date +%Y%m%d-%H%M)
    echo "$run_id" > "$ARTIFACTS/run-id.txt"
    configure_kafka_phase "$run_id" "$GOLD_FROM"
    start_app "{\"ApplicationRestoreType\":\"RESTORE_FROM_CUSTOM_SNAPSHOT\",\"SnapshotName\":\"$SNAPSHOT_DEMO\"}"
    echo "ready: Flink is RUNNING on transactions-$run_id, waiting for the replay"
    ;;

  demo)
    run_id=$(cat "$ARTIFACTS/run-id.txt")
    replay "$START" "$END" "$SPEED" "transactions-$run_id"
    echo "transactions-$run_id"
    ;;

  stop)
    stop_app
    echo "stopped gracefully"
    ;;

  redemo)
    if [ -f "$ARTIFACTS/run-id.txt" ]; then
      "$0" wipe "$(cat "$ARTIFACTS/run-id.txt")" >/dev/null 2>&1 \
        && echo "wiped previous run $(cat "$ARTIFACTS/run-id.txt")" \
        || echo "nothing to wipe for $(cat "$ARTIFACTS/run-id.txt")" >&2
    fi
    "$0" predemo
    "$0" demo
    ;;

  teardown)
    if [ "$(status 2>/dev/null || echo READY)" = "RUNNING" ]; then stop_app; fi

    if [ -f "$ARTIFACTS/run-id.txt" ]; then
      "$0" wipe "$(cat "$ARTIFACTS/run-id.txt")" >/dev/null 2>&1 \
        && echo "wiped demo run $(cat "$ARTIFACTS/run-id.txt") from $GOLD_TABLE" \
        || echo "no demo rows to wipe" >&2
      rm -f "$ARTIFACTS/run-id.txt"
    fi

    (cd infra && terraform destroy -input=false -auto-approve \
      -target aws_msk_cluster.main -target aws_vpc_endpoint.glue -target aws_instance.dashboard)
    echo "cluster and dashboard gone; backfill history, both snapshots, the Lambda, the network and S3 kept"
    ;;

  wipe)
    run_id="${2:-}"
    if [ -n "$run_id" ]; then
      statement="DELETE FROM $GLUE_DATABASE.$GOLD_TABLE WHERE run_id = '$run_id'"
    else
      statement="DROP TABLE IF EXISTS $GLUE_DATABASE.$GOLD_TABLE"
    fi
    query=$(aws athena start-query-execution \
      --work-group "${ATHENA_WORKGROUP:-aml-bq1}" \
      --query-string "$statement" \
      --query QueryExecutionId --output text)
    until state=$(aws athena get-query-execution --query-execution-id "$query" \
      --query 'QueryExecution.Status.State' --output text); [ "$state" != "RUNNING" ] && [ "$state" != "QUEUED" ]; do
      sleep 3
    done
    if [ "$state" != "SUCCEEDED" ]; then
      aws athena get-query-execution --query-execution-id "$query" \
        --query 'QueryExecution.Status.StateChangeReason' --output text >&2
      exit 1
    fi
    echo "$statement"
    ;;

  *)
    echo "usage: $0 {setup|predemo|demo|teardown} normal use, in that order
           $0 redemo  wipe the last run, then predemo and demo again
           {build|provision|deploy|warmup|backfill|run|smoke|stop|wipe} individual steps
           {dashboard-build|dashboard-redeploy} package the dashboard, or replace its instance
           {warmup-kafka|backfill-kafka} replay through MSK instead of reading S3" >&2
    exit 1
    ;;
esac
