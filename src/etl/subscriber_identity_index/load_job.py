# Job: subscriber_identity_index_load
# Description: AWS Glue (PySpark) job that reads the day's extracted Parquet
#   file from S3 and performs a diff-style incremental load into a
#   Redis/Valkey cache: a primary index (account ID -> full record) and two
#   secondary indexes (phone number and personal ID -> set of account IDs).
#   Only new/changed/removed entries are written per run; unchanged entries
#   are TTL-refreshed. Includes SNS alerting on failure and a post-load
#   validation-sampling step.
#
# This is a sanitized, standalone reconstruction of a production Glue job.
# Real AWS account IDs, bucket names, VPC/subnet/security-group IDs, and
# cluster endpoints have been replaced with placeholders / environment
# variables — see README "Author's Note".
"""
Suggested job configuration:
  Job type: Spark | Glue version: 5.0+ | Worker type: G.2X | Workers: 8
  Timeout: long-running (data volume dependent) | Retries: 1-2
  Network: attach a Glue connection with access to your ElastiCache VPC.
  Additional Python modules: redis
"""
import sys
import os
import time
import json
import hmac
import hashlib
import logging
import traceback
from collections import Counter
from datetime import datetime, timedelta

import boto3
import redis
import pytz
from awsglue.transforms import *  # noqa: F401,F403 (Glue convention)
from awsglue.utils import getResolvedOptions, GlueArgumentError
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, split, trim, col, collect_set, regexp_replace
from pyspark import StorageLevel

# --- Logging -----------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# --- Glue / Spark bootstrap ---------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job_name = args["JOB_NAME"]

# --- Configuration (environment-driven; defaults are placeholders) ------
job_tz = pytz.timezone(os.getenv("JOB_TIMEZONE", "UTC"))
load_offset_days = int(os.getenv("LOAD_OFFSET_DAYS", "2"))  # upstream data lands T-N days

try:
    load_day = getResolvedOptions(sys.argv, ["LOAD_DAY"])["LOAD_DAY"]
except GlueArgumentError:
    load_day = (datetime.now(job_tz) - timedelta(days=load_offset_days)).strftime("%Y-%m-%d")

S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", "your-data-lake-bucket")
S3_SOURCE_PATH = os.getenv(
    "S3_SOURCE_PATH",
    f"s3://{S3_SOURCE_BUCKET}/subscriber_identity_index/dt={load_day}/subscriber_identity_index_{load_day.replace('-', '')}.parquet",
)
S3_SOURCE_KEY = S3_SOURCE_PATH.replace(f"s3://{S3_SOURCE_BUCKET}/", "")

# Authentication and in-transit TLS are intentionally omitted from this
# reconstruction — configure them for your deployment rather than assuming
# network isolation (private subnet + security group) is enough on its own.
# It rules out access from outside the VPC but not from a compromised
# neighbor inside it; pair it with Redis AUTH/IAM auth and ssl=True unless
# you have a specific reason not to (see also src/api/header_lookup/handler.py).
REDIS_ENDPOINT = os.getenv("REDIS_ENDPOINT", "your-cluster.xxxxxxx.use1.cache.amazonaws.com")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL_DAYS = int(os.getenv("REDIS_TTL_DAYS", "7"))  # example default — tune to your data's staleness tolerance
REDIS_KEY_TTL_SECONDS = REDIS_TTL_DAYS * 24 * 3600
REDIS_BATCH_SIZE = int(os.getenv("REDIS_BATCH_SIZE", "2000"))
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))

REDIS_CONFIG = {
    "host": REDIS_ENDPOINT,
    "port": REDIS_PORT,
    "db": REDIS_DB,
    "max_connections": REDIS_MAX_CONNECTIONS,
    "socket_connect_timeout": 5,
    "socket_timeout": 15,
    "socket_keepalive": True,
    "retry_on_timeout": True,
    "health_check_interval": 30,
    "decode_responses": True,
}

SNS_TOPIC_ARN_ALERTS = os.getenv("SNS_TOPIC_ARN_ALERTS", "arn:aws:sns:us-east-1:123456789012:pipeline-alerts")
NUM_PARTITIONS = int(os.getenv("SPARK_NUM_PARTITIONS", "64"))  # tune to ~2-4x executor cores

# Used only to build a short, non-reversible token for correlating log lines
# about a specific key without ever printing the identifier (account ID,
# phone number, personal ID) it's built from. A *plain* hash would not
# actually be one-way here: phone numbers and account IDs have a small
# enough input space to enumerate and reverse a bare SHA-256 in practice,
# so this is HMAC'd with a secret instead of hashed bare — which only holds
# if that secret is unknown, so there's deliberately no insecure fallback
# below. Set LOG_KEY_HASH_SECRET from Secrets Manager or a Glue job
# parameter; the job refuses to start without it.
LOG_KEY_HASH_SECRET = os.getenv("LOG_KEY_HASH_SECRET")
if not LOG_KEY_HASH_SECRET:
    logger.error(
        "LOG_KEY_HASH_SECRET is not set. Refusing to start: a known or "
        "default secret makes the log-key fingerprint below enumerable, "
        "which defeats the point of not logging the raw identifier."
    )
    sys.exit(1)
LOG_KEY_HASH_SECRET = LOG_KEY_HASH_SECRET.encode()

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def _redacted_key(key: str) -> str:
    """'acct_id:55501234567' -> 'acct_id:9f2a1c4b7e01' — keep the index type
    (useful for triage: which index, which code path) and drop the
    identifier value, replacing it with a short keyed-hash fingerprint that's
    stable across log lines (so repeated failures on the same key are still
    correlatable) but not reversible to the original account/phone/ID."""
    prefix, _, value = key.partition(":")
    fingerprint = hmac.new(LOG_KEY_HASH_SECRET, value.encode(), hashlib.sha256).hexdigest()[:12]
    return f"{prefix}:{fingerprint}"


# --- Utilities -----------------------------------------------------------

def optimize_spark_config(spark_session):
    """Adaptive-execution and Arrow settings tuned for a wide diff/index
    build followed by many small Redis writes rather than a single large
    shuffle-heavy aggregation."""
    settings = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
        "spark.sql.files.maxPartitionBytes": str(128 * 1024 * 1024),
    }
    for key, value in settings.items():
        try:
            spark_session.conf.set(key, value)
        except Exception as e:
            logger.warning(f"Could not set {key}: {e}")
    logger.info(f"Spark version: {spark_session.version}")


def send_sns_alert(subject, message, error_type=None, operation=None):
    try:
        full_message = (
            f"Job Name: {job_name}\n"
            f"Timestamp: {datetime.now().isoformat()}\n"
            f"Error Type: {error_type or 'N/A'}\n"
            f"Failed Operation: {operation or 'N/A'}\n"
            f"Processing Date: {load_day}\n\n"
            f"Error Details:\n{message.strip()}"
        )
        response = sns_client.publish(TopicArn=SNS_TOPIC_ARN_ALERTS, Subject=subject, Message=full_message)
        logger.info(f"SNS alert sent: {response['MessageId']}")
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {e}")


def fail_job(subject, error_msg, error_type, operation):
    logger.error(error_msg)
    send_sns_alert(subject=subject, message=error_msg, error_type=error_type, operation=operation)
    raise Exception(error_msg)


# --- Diff-style index writers (run per Spark partition) ------------------

def update_primary_index_diff(rows):
    """
    Diff-style update for the primary index: acct_id:<account_id> -> JSON record.

    For each row:
      - key absent in Redis         -> SET with fresh TTL              (new)
      - key present, value unchanged -> EXPIRE only, TTL refresh        (unchanged)
      - key present, value changed  -> SET with fresh TTL               (overwritten)
    Rows whose account is no longer in today's dataset are left alone and
    expire naturally via TTL — there is no explicit delete path, which
    keeps this idempotent and safe to re-run.
    """
    cfg = redis_config_bc.value
    pool = redis.ConnectionPool(**cfg)
    ttl = redis_ttl_bc.value
    batch_size = batch_size_bc.value
    conn = redis.StrictRedis(connection_pool=pool)
    pipe = conn.pipeline()

    metrics = Counter()
    processed = 0
    start = time.time()

    for row in rows:
        acct_id = row["account_id"]
        if not acct_id:
            continue
        key = f"acct_id:{acct_id.strip()}"
        row_dict = row.asDict() if hasattr(row, "asDict") else row
        new_value = json.dumps(row_dict, sort_keys=True)

        try:
            current_value = conn.get(key)
        except Exception as e:
            logger.warning(f"Could not fetch Redis key {_redacted_key(key)}: {e}")
            current_value = None

        if current_value is None:
            pipe.set(key, new_value, ex=ttl)
            metrics["keys_new"] += 1
        elif current_value == new_value:
            pipe.expire(key, ttl)
            metrics["keys_unchanged"] += 1
        else:
            pipe.set(key, new_value, ex=ttl)
            metrics["keys_overwritten"] += 1

        processed += 1
        if processed % batch_size == 0:
            _safe_execute(pipe, "primary index")
            time.sleep(0.01)  # brief pause between batches to avoid saturating Redis

    _safe_execute(pipe, "primary index")
    conn.close()

    metrics["total_keys_processed"] = processed
    metrics["processing_time_seconds"] = time.time() - start
    return [(processed, dict(metrics))]


def update_secondary_index_diff(rows, redis_prefix):
    """
    Diff-style update for a secondary index (e.g. 'phone:' or 'pid:' ->
    Redis Set of account IDs).

    Reconciles Redis Set membership against the day's dataset:
      - not in Redis, present today   -> SADD + EXPIRE           (created)
      - in Redis, absent today        -> left alone to expire    (skipped)
      - membership identical          -> EXPIRE only             (refreshed)
      - membership differs            -> SADD new / SREM stale, then EXPIRE

    The explicit SREM on stale members matters: TTL alone only expires the
    whole set key, so a member that's no longer associated with this
    secondary ID (e.g. a phone number reassigned to a different account)
    would otherwise linger in the set until the *entire* key's TTL lapses.
    """
    cfg = redis_config_bc.value
    pool = redis.ConnectionPool(**cfg)
    ttl = redis_ttl_bc.value
    batch_size = batch_size_bc.value
    conn = redis.StrictRedis(connection_pool=pool)
    pipe = conn.pipeline()

    metrics = Counter()
    processed = 0
    start = time.time()

    for row in rows:
        row = row.asDict() if hasattr(row, "asDict") else row
        sec_id = row["secondary_index"]
        if not sec_id:
            continue
        redis_key = f"{redis_prefix}{sec_id.strip()}"
        new_members = set(row["acct_ids"])

        try:
            current_members = set(conn.smembers(redis_key))
        except Exception as e:
            logger.warning(f"Could not fetch Redis set for {_redacted_key(redis_key)}: {e}")
            current_members = set()

        if not current_members and new_members:
            pipe.sadd(redis_key, *new_members)
            pipe.expire(redis_key, ttl)
            metrics["keys_created"] += 1
            metrics["members_added"] += len(new_members)
        elif current_members and not new_members:
            metrics["keys_skipped"] += 1
        elif current_members == new_members:
            pipe.expire(redis_key, ttl)
            metrics["keys_refreshed"] += 1
        else:
            to_add = new_members - current_members
            to_remove = current_members - new_members
            if to_add:
                pipe.sadd(redis_key, *to_add)
                metrics["members_added"] += len(to_add)
            if to_remove:
                pipe.srem(redis_key, *to_remove)
                metrics["members_removed"] += len(to_remove)
            pipe.expire(redis_key, ttl)
            metrics["keys_refreshed"] += 1

        processed += 1
        if processed % batch_size == 0:
            _safe_execute(pipe, "secondary index")
            time.sleep(0.01)

    _safe_execute(pipe, "secondary index")
    conn.close()

    metrics["total_keys_processed"] = processed
    metrics["processing_time_seconds"] = time.time() - start
    return [(processed, dict(metrics))]


def _safe_execute(pipe, label):
    """Log the failing pipeline for triage, then re-raise. A caller that
    swallowed this would report every batch as processed even when the
    Redis writes in it never landed — the partition function's return value
    (used to build the per-index metrics and total_records count) has no
    way to distinguish a batch that executed from one that silently
    failed. Letting the exception propagate fails the Spark task, which
    fails the job via the fail_job() handler around the calling code —
    the correct outcome for a partial write."""
    try:
        pipe.execute()
    except Exception as e:
        logger.exception(f"Error executing {label} Redis pipeline: {e}")
        raise


def _aggregate(partition_metrics):
    total = sum(count for count, _ in partition_metrics)
    counter = Counter()
    for _, m in partition_metrics:
        counter.update(m)
    return total, dict(counter)


def _validate_sample(df, id_col, key_fn, redis_conn, sample_size=3, is_set=False):
    """Spot-check a handful of keys per index after load — a cheap
    correctness signal that doesn't require reading the whole dataset back."""
    for row in df.select(id_col).rdd.takeSample(False, sample_size):
        value = row[id_col]
        key = key_fn(value)
        found = redis_conn.smembers(key) if is_set else redis_conn.get(key)
        ttl = redis_conn.ttl(key)
        if not found:
            logger.warning(f"Validation: key {_redacted_key(key)} not found or empty in Redis!")
        else:
            logger.info(f"Validation: key {_redacted_key(key)} present (ttl={ttl}s).")


# --- Main -----------------------------------------------------------------

logger.info(f"Starting Glue job: {job_name} | load_day={load_day} | source={S3_SOURCE_PATH}")

sc = SparkContext.getOrCreate()
redis_config_bc = sc.broadcast(REDIS_CONFIG)
redis_ttl_bc = sc.broadcast(REDIS_KEY_TTL_SECONDS)
batch_size_bc = sc.broadcast(REDIS_BATCH_SIZE)
optimize_spark_config(spark)

try:
    s3_client.head_object(Bucket=S3_SOURCE_BUCKET, Key=S3_SOURCE_KEY)
except Exception as e:
    fail_job(f"[CRITICAL] Source file not found in {job_name}",
              f"Source Parquet file not found at {S3_SOURCE_PATH}. Error: {e}",
              "FileNotFoundError", "File existence check")

try:
    redis_prod = redis.StrictRedis(connection_pool=redis.ConnectionPool(**REDIS_CONFIG))
    mem_before = redis_prod.info("memory")["used_memory"]
    logger.info(f"Connected to Redis. DB size before job: {redis_prod.dbsize()}, "
                f"memory: {mem_before / 1024**3:.2f} GiB")
except redis.ConnectionError as e:
    fail_job(f"[CRITICAL] Redis connection failed in {job_name}", f"Failed to connect to Redis: {e}",
              "ConnectionError", "Redis connection")

try:
    # 1) Read + normalize -------------------------------------------------
    try:
        df_primary = spark.read.parquet(S3_SOURCE_PATH)
    except Exception as e:
        fail_job(f"[CRITICAL] Parquet read failed in {job_name}",
                  f"Failed to read parquet file from S3: {S3_SOURCE_PATH}. Error: {e}",
                  "FileReadError", "Parquet file read")

    if df_primary.rdd.isEmpty():
        fail_job(f"[CRITICAL] Empty DataFrame in {job_name}",
                  "DataFrame is empty after reading the parquet file.", "DataFrameError", "DataFrame processing")

    df_primary = df_primary.select(*[col(c).cast("string").alias(c) for c in df_primary.columns])
    df_primary = df_primary.filter(col("account_id").isNotNull())
    df_primary = df_primary.persist(StorageLevel.MEMORY_AND_DISK)

    required_cols = ["fixed_svc_phone_list", "mobile_svc_phone_list", "contact_phone_list", "personal_id"]
    missing = [c for c in required_cols if c not in df_primary.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # 2) Build secondary indexes ------------------------------------------
    def explode_phones(df, source_col):
        return (
            df.withColumn("phone", explode(split(col(source_col), ",")))
              .withColumn("phone", trim(col("phone")))
              .select("phone", "account_id")
        )

    df_phones = (
        explode_phones(df_primary, "fixed_svc_phone_list")
        .union(explode_phones(df_primary, "mobile_svc_phone_list"))
        .union(explode_phones(df_primary, "contact_phone_list"))
        .filter(col("phone") != "")
    )
    df_phones = df_phones.withColumn("phone", regexp_replace(trim(col("phone")), "[^0-9]", "")) \
                          .filter(col("phone").rlike("^[0-9]+$"))
    df_phone_index = df_phones.groupBy("phone").agg(collect_set("account_id").alias("acct_ids"))
    logger.info(f"Phone index: {df_phone_index.count()} keys")

    df_ids = (
        df_primary.withColumn("personal_id", trim(col("personal_id")))
        .filter(col("personal_id").isNotNull() & (col("personal_id") != ""))
        .select(col("personal_id").alias("pid"), col("account_id"))
    )
    df_pid_index = df_ids.groupBy("pid").agg(collect_set("account_id").alias("acct_ids"))
    logger.info(f"Personal ID index: {df_pid_index.count()} keys")

    # 3) Load into Redis (primary, then phone, then personal ID) ----------
    total_records = 0
    all_metrics = {}

    try:
        df_partitioned = df_primary.repartition(NUM_PARTITIONS)
        processed, metrics = _aggregate(df_partitioned.rdd.mapPartitions(update_primary_index_diff).collect())
    except Exception as e:
        fail_job(f"[CRITICAL] Primary index load failed in {job_name}",
                  f"Primary index load failed: {e}\n{traceback.format_exc()}", "DataLoadError", "Primary index load")
    total_records += processed
    all_metrics["primary"] = metrics
    logger.info(f"Primary index load complete: {processed} records. {metrics}")

    for label, df_index, prefix in (
        ("phone", df_phone_index, "phone:"),
        ("personal_id", df_pid_index, "pid:"),
    ):
        try:
            df_for_diff = (
                df_index.withColumnRenamed(df_index.columns[0], "secondary_index")
                .persist(StorageLevel.MEMORY_AND_DISK)
                .repartition(NUM_PARTITIONS)
            )
            processed, metrics = _aggregate(
                df_for_diff.rdd.mapPartitions(lambda rows, p=prefix: update_secondary_index_diff(rows, p)).collect()
            )
        except Exception as e:
            fail_job(f"[CRITICAL] {label} index load failed in {job_name}",
                      f"{label} index load failed: {e}\n{traceback.format_exc()}", "DataLoadError", f"{label} index load")
        total_records += processed
        all_metrics[label] = metrics
        logger.info(f"{label} secondary index load complete: {processed} records. {metrics}")

    # 4) Post-load validation sampling ------------------------------------
    try:
        _validate_sample(df_primary, "account_id", lambda v: f"acct_id:{v.strip()}", redis_prod)
        _validate_sample(df_phone_index, "phone", lambda v: f"phone:{v.strip()}", redis_prod, is_set=True)
        _validate_sample(df_pid_index, "pid", lambda v: f"pid:{v.strip()}", redis_prod, is_set=True)
        logger.info("Redis DB validation sampling passed.")
    except Exception as e:
        fail_job(f"[CRITICAL] Redis validation failed in {job_name}",
                  f"Redis DB validation failed: {e}\n{traceback.format_exc()}", "ValidationError", "Redis DB validation")

    logger.info(f"Data load completed successfully. Total records processed: {total_records}")

except Exception as e:
    fail_job(f"[CRITICAL] Redis data loading failed in {job_name}",
              f"Data loading to Redis failed: {e}\n{traceback.format_exc()}", "DataLoadingError", "Data loading to Redis")

finally:
    try:
        mem_after = redis_prod.info("memory")["used_memory"]
        logger.info(f"Load metrics: {json.dumps(all_metrics, indent=2)}" if "all_metrics" in dir() else "")
        logger.info(f"Job finished. DB size after job: {redis_prod.dbsize()}, memory: {mem_after / 1024**3:.2f} GiB")
        job.commit()
    except Exception as e:
        logger.warning(f"Job commit / final logging failed: {e}")
