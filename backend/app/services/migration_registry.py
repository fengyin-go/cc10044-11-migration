"""Hive→Iceberg 迁移登记表：表分区映射、核验记录与读路径状态机。

数据链路位置：迁移控制层。``data_migration`` 服务在这里登记并推进迁移批次，
``lineage_graph`` 服务在这里读取经过核验的表分区映射，为取数流水线和报表
解析取数地址。

设计约束（对应迁移上线规约）：

- 比对结果必须标清表和目标数据版本（Iceberg snapshot id）；
- 漏一日（缺分区）或行数核验不符的表不能当作已迁完；
- 切换前读路径保持 Hive 存量可读；回滚后原 Hive 分区继续可读，
  已产生的 Iceberg 核验记录保留不删；
- 联合取数时，同一结果中的全部输入地址必须来自同一套完整可读的数据，
  任一表未就绪则整体回退到 Hive 存量读路径。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# 读路径
READ_HIVE = "hive"
READ_ICEBERG = "iceberg"

# 分区核验状态
STATUS_VERIFIED = "已核验"
STATUS_MISSING = "缺失"
STATUS_PENDING = "待核验"
STATUS_MISMATCH = "核验不符"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _partition(
    table: str,
    dt: str,
    hive_rows: int,
    iceberg_rows: int | None,
    verify_status: str,
    target_version: str | None,
    verified_at: str | None,
) -> dict[str, Any]:
    return {
        "table": table,
        "partition": f"dt={dt}",
        "hive_path": f"hdfs://nn-prod:8020/warehouse/orders/{table}/dt={dt}",
        "iceberg_path": f"s3://lakehouse-iceberg/orders/{table}/dt={dt}",
        "hive_rows": hive_rows,
        "iceberg_rows": iceberg_rows,
        "verify_status": verify_status,
        "target_version": target_version,
        "verified_at": verified_at,
    }


def _seed_batch() -> dict[str, Any]:
    """迁移批次 MB-2026-11-21-orders 的初始状态。

    ods_orders 四个分区全部核验通过；dwd_orders_clean 缺 dt=2026-11-21
    一个分区（漏一日），整表不能算作已迁完。读路径尚未切换，取数仍走
    Hive 存量读路径。
    """
    days = ["2026-11-18", "2026-11-19", "2026-11-20", "2026-11-21"]
    ods_rows = {"2026-11-18": 152340, "2026-11-19": 148920,
                "2026-11-20": 151080, "2026-11-21": 149760}
    dwd_rows = {"2026-11-18": 151980, "2026-11-19": 148602,
                "2026-11-20": 150733, "2026-11-21": 149401}
    ods_parts = [
        _partition("ods_orders", day, ods_rows[day], ods_rows[day], STATUS_VERIFIED,
                   f"iceberg-snapshot-{8101 + idx}", f"2026-11-22T02:1{idx}:00Z")
        for idx, day in enumerate(days)
    ]
    dwd_parts = [
        _partition("dwd_orders_clean", day, dwd_rows[day], dwd_rows[day], STATUS_VERIFIED,
                   f"iceberg-snapshot-{8201 + idx}", f"2026-11-22T03:1{idx}:00Z")
        for idx, day in enumerate(days[:3])
    ]
    dwd_parts.append(
        _partition("dwd_orders_clean", "2026-11-21", dwd_rows["2026-11-21"],
                   None, STATUS_MISSING, None, None)
    )
    return {
        "batch_id": "MB-2026-11-21-orders",
        "task_id": 1,
        "task_name": "Hive到Iceberg订单迁移",
        "source_engine": READ_HIVE,
        "target_engine": READ_ICEBERG,
        "read_mode": READ_HIVE,
        "tables": {"ods_orders": ods_parts, "dwd_orders_clean": dwd_parts},
        "switch_history": [],
    }


class MigrationRegistry:
    """迁移批次登记表：分区映射、核验记录、读路径切换与回滚。"""

    def __init__(self) -> None:
        self._batches: dict[int, dict[str, Any]] = {}
        self._version_seq = 9000
        self.register(_seed_batch())

    # ---- 批次查询 -----------------------------------------------------

    def register(self, batch: dict[str, Any]) -> None:
        self._batches[int(batch["task_id"])] = batch

    def batch_for_task(self, task_id: int) -> dict[str, Any] | None:
        return self._batches.get(int(task_id))

    def batch_for_table(self, table: str) -> dict[str, Any] | None:
        for batch in self._batches.values():
            if table in batch["tables"]:
                return batch
        return None

    def next_version(self) -> str:
        self._version_seq += 1
        return f"iceberg-snapshot-{self._version_seq}"

    # ---- 核验状态推导 -------------------------------------------------

    @staticmethod
    def table_fully_verified(batch: dict[str, Any], table: str) -> bool:
        """整表所有分区均核验通过才算已迁完；漏一日或核验不符都不算。"""
        parts = batch["tables"].get(table) or []
        return bool(parts) and all(
            p["verify_status"] == STATUS_VERIFIED for p in parts
        )

    @staticmethod
    def incomplete_partitions(batch: dict[str, Any], table: str) -> list[dict[str, Any]]:
        return [
            p for p in batch["tables"].get(table, [])
            if p["verify_status"] != STATUS_VERIFIED
        ]

    def batch_fully_verified(self, batch: dict[str, Any]) -> bool:
        return all(self.table_fully_verified(batch, t) for t in batch["tables"])

    # ---- 工作流操作 ---------------------------------------------------

    def verify_table(self, batch: dict[str, Any], table: str) -> list[dict[str, Any]]:
        """逐分区比对 Hive 与 Iceberg 行数，结果标清表和目标数据版本。"""
        checked_at = _now()
        results: list[dict[str, Any]] = []
        for part in batch["tables"][table]:
            if part["iceberg_rows"] is None:
                part["verify_status"] = STATUS_MISSING
                match, reason = False, "目标端缺该分区（漏一日），不能当作已迁完"
            elif part["iceberg_rows"] != part["hive_rows"]:
                part["verify_status"] = STATUS_MISMATCH
                match, reason = False, "目标端行数与源端不符"
            else:
                part["verify_status"] = STATUS_VERIFIED
                if not part["target_version"]:
                    part["target_version"] = self.next_version()
                part["verified_at"] = checked_at
                match, reason = True, "行数一致"
            results.append({
                "table": table,
                "partition": part["partition"],
                "target_version": part["target_version"],
                "hive_rows": part["hive_rows"],
                "iceberg_rows": part["iceberg_rows"],
                "verify_status": part["verify_status"],
                "match": match,
                "reason": reason,
                "checked_at": checked_at,
            })
        return results

    def backfill_partition(
        self, batch: dict[str, Any], table: str, partition: str
    ) -> dict[str, Any] | None:
        """补迁缺失或不符的分区：按 Hive 分区重写 Iceberg，写后处于待核验。"""
        for part in batch["tables"].get(table, []):
            if part["partition"] == partition:
                part["iceberg_rows"] = part["hive_rows"]
                part["target_version"] = self.next_version()
                part["verify_status"] = STATUS_PENDING
                part["verified_at"] = None
                return part
        return None

    def switch_to_iceberg(self, batch: dict[str, Any]) -> None:
        batch["read_mode"] = READ_ICEBERG
        batch["switch_history"].append({
            "action": "切换", "read_mode": READ_ICEBERG, "at": _now(),
        })

    def rollback_to_hive(self, batch: dict[str, Any]) -> None:
        """回滚只翻动读路径：Hive 分区保持可读，Iceberg 核验记录原样保留。"""
        batch["read_mode"] = READ_HIVE
        batch["switch_history"].append({
            "action": "回滚", "read_mode": READ_HIVE, "at": _now(),
        })

    # ---- 取数地址解析 -------------------------------------------------

    def resolve_table_address(self, batch: dict[str, Any], table: str) -> dict[str, Any]:
        """单表取数地址：按当前读路径和核验结果解析。

        读路径已切 Iceberg 且整表核验通过 → 返回 Iceberg 地址（带目标版本）；
        否则回退 Hive 存量分区，保证取数侧始终拿到完整可读的数据。
        """
        parts = batch["tables"][table]
        verified = self.table_fully_verified(batch, table)
        if batch["read_mode"] == READ_ICEBERG and verified:
            return {
                "table": table,
                "mapped": True,
                "engine": READ_ICEBERG,
                "complete": True,
                "addresses": [
                    {
                        "partition": p["partition"],
                        "address": p["iceberg_path"],
                        "rows": p["iceberg_rows"],
                        "target_version": p["target_version"],
                    }
                    for p in parts
                ],
                "reason": "读路径已切换，按核验通过的 Iceberg 分区映射取数",
            }
        reason = (
            "读路径未切换，走 Hive 存量读路径"
            if batch["read_mode"] == READ_HIVE
            else "整表未核验通过（漏分区或核验不符），回退 Hive 存量读路径"
        )
        return {
            "table": table,
            "mapped": True,
            "engine": READ_HIVE,
            "complete": True,
            "addresses": [
                {"partition": p["partition"], "address": p["hive_path"],
                 "rows": p["hive_rows"], "target_version": None}
                for p in parts
            ],
            "reason": reason,
        }

    def resolve_joint_address(
        self, batch: dict[str, Any], tables: list[str]
    ) -> dict[str, Any]:
        """联合取数地址：同一结果的全部输入必须来自一套完整可读的数据。

        所有已登记表都能按 Iceberg 服务时才整体走 Iceberg；任一表只能走
        Hive（未切换/未整表核验通过）时，全部输入统一回退 Hive 存量分区，
        不允许同一结果混用两个引擎的地址。
        """
        resolved = [self.resolve_table_address(batch, t) for t in tables]
        blocking = [r["table"] for r in resolved if r["engine"] != READ_ICEBERG]
        if blocking:
            resolved = [
                self._force_hive(batch, r) if r["engine"] == READ_ICEBERG else r
                for r in resolved
            ]
            engine = READ_HIVE
            note = (
                f"表 {','.join(blocking)} 未就绪，联合取数整体回退 Hive 存量分区，"
                "保证同一结果的输入地址来自一套完整可读的数据"
            )
        else:
            engine = READ_ICEBERG
            note = "全部输入均按核验通过的 Iceberg 分区映射解析，同属一套完整可读数据"
        return {
            "engine": engine,
            "consistent": True,
            "tables": resolved,
            "note": note,
        }

    def _force_hive(self, batch: dict[str, Any], resolved: dict[str, Any]) -> dict[str, Any]:
        parts = batch["tables"][resolved["table"]]
        return {
            "table": resolved["table"],
            "mapped": True,
            "engine": READ_HIVE,
            "complete": True,
            "addresses": [
                {"partition": p["partition"], "address": p["hive_path"],
                 "rows": p["hive_rows"], "target_version": None}
                for p in parts
            ],
            "reason": "联合取数一致性回退：与未就绪表保持同一套 Hive 存量数据",
        }


migration_registry = MigrationRegistry()
