"""Bronze: raw ride events from Azure Event Hubs (Kafka endpoint).

Pipeline configuration keys (set in databricks.yml or the pipeline UI):
    catalog              Unity Catalog catalog, default "rides"
    eventhub_namespace   Event Hubs namespace, e.g. "rides-events"
    eventhub_name        Event hub (topic) name, e.g. "rides"
    secret_scope         Databricks secret scope holding the connection string
    secret_key           key inside that scope, default "eventhub-connection-string"

The connection string is read from a secret scope - never hard-coded.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col

CATALOG = spark.conf.get("catalog", "rides")
EH_NAMESPACE = spark.conf.get("eventhub_namespace")
EH_NAME = spark.conf.get("eventhub_name")
EH_CONN_STR = dbutils.secrets.get(
    scope=spark.conf.get("secret_scope"),
    key=spark.conf.get("secret_key", "eventhub-connection-string"),
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{EH_CONN_STR}";'
    ),
    "kafka.request.timeout.ms": "10000",
    "kafka.session.timeout.ms": "10000",
    "maxOffsetsPerTrigger": "10000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}


@dp.table(
    name=f"{CATALOG}.bronze.rides_raw",
    comment="Raw ride events from Event Hubs, payload kept as a JSON string plus Kafka metadata",
)
def rides_raw():
    return (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
        .select(
            col("value").cast("string").alias("rides"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("enqueued_at"),
        )
    )
