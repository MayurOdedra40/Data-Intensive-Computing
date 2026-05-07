"""SparkSession builder shared across Assignment 2 parts.

`mode="local"` configures a local[*] master suitable for laptop development.
`mode="cluster"` defers master selection to ``spark-submit`` so YARN-cluster
mode works without local-only flags leaking through.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str, mode: str = "local") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if mode == "local":
        builder = builder.master("local[*]")
        # WSL2's resolved hostname can fail to bind; pin to loopback.
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
        builder = builder.config("spark.driver.host", "127.0.0.1")
    elif mode != "cluster":
        raise ValueError(f"Unknown mode: {mode!r}")

    builder = builder.config(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer",
    )
    return builder.getOrCreate()


__all__ = ["build_spark"]
