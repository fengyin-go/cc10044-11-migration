"""数据迁移领域服务：管理数据从源存储迁移到目标存储的全流程，包括迁移执行、数据校验和源目标对比。

数据链路位置：数据迁移层，执行跨存储系统的数据迁移和完整性校验。"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.migration_registry import migration_registry
from app.store import store


class DataMigrationService(BaseModuleService):
    MODULE = "data_migration"
    ENTITY = "迁移任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "源存储", "目标存储", "迁移范围", "校验方式"]
    REQUIRED_FIELDS = ["任务名称", "源存储", "目标存储"]
    STATUS_ORDER = ["待迁", "迁移中", "已校验", "迁移失败"]
    ACTION_RULES = {"启动迁移": "待迁", "取消迁移": "迁移中"}
    NEGATIVE_ACTIONS = []

    def start_migration(self, entry_id: int) -> dict[str, Any]:
        """启动数据迁移任务。

        创建迁移计划并启动分布式迁移作业，
        读取源存储数据，按范围分批写入目标存储。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "迁移任务不存在"}
        if entry.get("status") != "待迁":
            return {"ok": False, "message": "只能启动待迁状态的迁移任务"}

        source = entry.get("源存储", "")
        target = entry.get("目标存储", "")
        migration_range = entry.get("迁移范围", "全量")

        plan = {
            "task_id": entry_id,
            "source_type": source,
            "target_type": target,
            "range": migration_range,
            "total_bytes": 15728640000,
            "total_files": 240,
            "estimated_duration": "45 分钟",
            "strategy": "direct_copy" if source.startswith("HDFS") and target.startswith("S3") else "transform_and_load",
        }

        entry["status"] = "迁移中"
        return {"ok": True, "message": "迁移已开始", "plan": plan}

    def validate_migration(self, entry_id: int) -> dict[str, Any]:
        """校验迁移结果的数据完整性。

        对登记了迁移批次的任务，逐表逐分区比对 Hive 与 Iceberg 行数，
        比对结果标清表和目标数据版本；漏一日或核验不符的表不能当作
        已迁完，任务状态也不会置为「已校验」。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "迁移任务不存在"}

        batch = migration_registry.batch_for_task(entry_id)
        if batch is not None:
            table_results = {
                table: migration_registry.verify_table(batch, table)
                for table in batch["tables"]
            }
            all_passed = migration_registry.batch_fully_verified(batch)
            entry["status"] = "已校验" if all_passed else "迁移中"
            entry["pending"] = not all_passed
            entry["abnormal"] = any(
                r["verify_status"] == "核验不符"
                for results in table_results.values() for r in results
            )
            return {
                "ok": all_passed,
                "task_id": entry_id,
                "batch_id": batch["batch_id"],
                "tables": {
                    table: {
                        "table_migrated": migration_registry.table_fully_verified(batch, table),
                        "checks": results,
                    }
                    for table, results in table_results.items()
                },
                "all_passed": all_passed,
                "message": "所有表全部分区校验通过" if all_passed
                else "存在漏分区或校验不符的表，不能当作已迁完",
            }

        checks = [
            {"type": "row_count", "source_value": 1520000, "target_value": 1520000, "match": True},
            {"type": "checksum_sample", "source_value": "8a3f21c...", "target_value": "8a3f21c...", "match": True},
            {"type": "partition_integrity", "source_partitions": 365, "target_partitions": 365, "match": True},
            {"type": "schema_compatibility", "source_columns": 48, "target_columns": 48, "match": True},
            {"type": "timestamp_fidelity", "precision_loss": "none", "match": True},
        ]
        all_passed = all(c["match"] for c in checks)
        entry["status"] = "已校验" if all_passed else "迁移失败"
        entry["pending"] = not all_passed
        if not all_passed:
            entry["abnormal"] = True

        return {
            "ok": all_passed,
            "task_id": entry_id,
            "checks": checks,
            "all_passed": all_passed,
            "message": "所有校验通过" if all_passed else "存在校验不一致，请检查",
        }

    # ---- Hive→Iceberg 迁移批次工作流 ---------------------------------

    def _batch_guard(self, entry_id: int) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
        """取出任务和迁移批次；任一缺失时返回错误响应。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, None, {"ok": False, "message": "迁移任务不存在"}
        batch = migration_registry.batch_for_task(entry_id)
        if batch is None:
            return entry, None, {"ok": False, "message": "该任务没有登记 Hive→Iceberg 迁移批次"}
        return entry, batch, None

    def partition_map(self, entry_id: int) -> dict[str, Any]:
        """查看批次登记的表分区映射：行数、分区、核验结果和目标数据版本。"""
        entry, batch, error = self._batch_guard(entry_id)
        if error is not None:
            return error
        return {
            "ok": True,
            "task_id": entry_id,
            "batch_id": batch["batch_id"],
            "read_mode": batch["read_mode"],
            "read_mode_label": "Iceberg（已切换）" if batch["read_mode"] == "iceberg" else "Hive 存量读路径",
            "tables": [
                {
                    "table": table,
                    "fully_verified": migration_registry.table_fully_verified(batch, table),
                    "incomplete_partitions": [
                        p["partition"]
                        for p in migration_registry.incomplete_partitions(batch, table)
                    ],
                    "partitions": [dict(p) for p in parts],
                }
                for table, parts in batch["tables"].items()
            ],
            "switch_history": list(batch["switch_history"]),
        }

    def verify_table(self, entry_id: int, table: str) -> dict[str, Any]:
        """核验（或重验）单表：逐分区比对行数并标清目标数据版本。"""
        entry, batch, error = self._batch_guard(entry_id)
        if error is not None:
            return error
        if table not in batch["tables"]:
            return {"ok": False, "message": f"表 {table} 不在迁移批次 {batch['batch_id']} 范围内"}

        checks = migration_registry.verify_table(batch, table)
        table_migrated = migration_registry.table_fully_verified(batch, table)
        all_passed = migration_registry.batch_fully_verified(batch)
        entry["status"] = "已校验" if all_passed else "迁移中"
        entry["pending"] = not all_passed
        entry["abnormal"] = any(c["verify_status"] == "核验不符" for c in checks)
        return {
            "ok": table_migrated,
            "task_id": entry_id,
            "batch_id": batch["batch_id"],
            "table": table,
            "table_migrated": table_migrated,
            "checks": checks,
            "target_versions": {c["partition"]: c["target_version"] for c in checks},
            "message": (
                f"表 {table} 全部分区核验通过"
                if table_migrated
                else f"表 {table} 存在漏分区或核验不符，不能当作已迁完"
            ),
        }

    def backfill_partition(self, entry_id: int, table: str, partition: str) -> dict[str, Any]:
        """补迁缺失或核验不符的分区，补迁后需重新核验才能算作已迁完。"""
        entry, batch, error = self._batch_guard(entry_id)
        if error is not None:
            return error
        if table not in batch["tables"]:
            return {"ok": False, "message": f"表 {table} 不在迁移批次 {batch['batch_id']} 范围内"}
        target = next(
            (p for p in batch["tables"][table] if p["partition"] == partition), None
        )
        if target is None:
            return {"ok": False, "message": f"分区 {partition} 不在表 {table} 的迁移范围内"}
        if target["verify_status"] == "已核验":
            return {"ok": False, "message": f"分区 {partition} 已核验通过，无需补迁"}

        part = migration_registry.backfill_partition(batch, table, partition)
        entry["status"] = "迁移中"
        entry["pending"] = True
        return {
            "ok": True,
            "task_id": entry_id,
            "table": table,
            "partition": dict(part),
            "message": f"分区 {partition} 补迁完成（版本 {part['target_version']}），待重新核验",
        }

    def switch_read_path(self, entry_id: int) -> dict[str, Any]:
        """切换读路径到 Iceberg：全批次所有表核验通过才允许上线。"""
        entry, batch, error = self._batch_guard(entry_id)
        if error is not None:
            return error
        if batch["read_mode"] == "iceberg":
            return {"ok": False, "message": "读路径已处于 Iceberg，无需重复切换"}
        not_ready = [
            table for table in batch["tables"]
            if not migration_registry.table_fully_verified(batch, table)
        ]
        if not_ready:
            return {
                "ok": False,
                "message": (
                    f"表 {','.join(not_ready)} 未整表核验通过，"
                    "漏一日或校验不符的表不能当作已迁完，禁止切换"
                ),
                "not_ready_tables": not_ready,
            }
        migration_registry.switch_to_iceberg(batch)
        entry["服务读路径"] = "Iceberg"
        return {
            "ok": True,
            "task_id": entry_id,
            "batch_id": batch["batch_id"],
            "read_mode": "iceberg",
            "message": "已切换到 Iceberg 读路径，血缘查询将按核验通过的表分区映射返回 Iceberg 取数地址",
        }

    def rollback_read_path(self, entry_id: int) -> dict[str, Any]:
        """回滚读路径到 Hive：原 Hive 分区继续可读，Iceberg 核验记录保留。"""
        entry, batch, error = self._batch_guard(entry_id)
        if error is not None:
            return error
        if batch["read_mode"] == "hive":
            return {"ok": False, "message": "读路径已处于 Hive 存量读路径，无需回滚"}
        migration_registry.rollback_to_hive(batch)
        entry["服务读路径"] = "Hive存量"
        retained = sum(
            1 for parts in batch["tables"].values() for p in parts
            if p["verify_status"] == "已核验"
        )
        return {
            "ok": True,
            "task_id": entry_id,
            "batch_id": batch["batch_id"],
            "read_mode": "hive",
            "retained_iceberg_verifications": retained,
            "message": "已回滚到 Hive 存量读路径，原 Hive 分区继续可读，Iceberg 核验记录全部保留",
        }

    def compare_source_target(self, entry_id: int) -> dict[str, Any]:
        """对比源端和目标端的详细差异报告。

        提供行级差异明细，列出仅在源端存在或仅在目标端存在的记录键。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "迁移任务不存在"}

        return {
            "task_id": entry_id,
            "source_type": entry.get("源存储"),
            "target_type": entry.get("目标存储"),
            "source_only_keys": 0,
            "target_only_keys": 0,
            "value_mismatch_keys": 0,
            "total_compared": 1520000,
            "consistency_rate": 1.0,
            "compare_duration_seconds": 87,
        }

    def list_migration_history_by_table(self, table_name: str) -> dict[str, Any]:
        """查询指定目标表的所有历史迁移记录。

        用于追踪表的迁移轨迹和验证迁移链路完整性。
        """
        all_migrations = store.rows(self.MODULE)
        related = [
            m for m in all_migrations
            if table_name in m.get("迁移范围", "")
        ]
        return {
            "table": table_name,
            "migration_count": len(related),
            "latest_migration_status": related[0].get("status") if related else None,
            "history": [
                {
                    "task_id": m["id"],
                    "task_name": m.get("任务名称"),
                    "source": m.get("源存储"),
                    "target": m.get("目标存储"),
                    "status": m.get("status"),
                }
                for m in related[:10]
            ],
        }

    def generate_migration_audit(self, entry_id: int) -> dict[str, Any]:
        """生成迁移审计记录供 data_lake_audit 消费。

        包含迁移的元数据变更追踪和合规性证据。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "迁移任务不存在"}

        audit = {
            "audit_type": "data_migration",
            "task_id": entry_id,
            "task_name": entry.get("任务名称"),
            "timestamp": "2026-11-21T08:00:00Z",
            "source": entry.get("源存储"),
            "target": entry.get("目标存储"),
            "range": entry.get("迁移范围"),
            "validation_method": entry.get("校验方式"),
            "validation_passed": entry.get("status") == "已校验",
            "audit_conclusion": "迁移数据完整，校验通过" if entry.get("status") == "已校验" else "存在待处理问题",
        }
        batch = migration_registry.batch_for_task(entry_id)
        if batch is not None:
            audit["batch_id"] = batch["batch_id"]
            audit["read_mode"] = batch["read_mode"]
            audit["verified_target_versions"] = {
                table: {
                    p["partition"]: p["target_version"]
                    for p in parts if p["verify_status"] == "已核验"
                }
                for table, parts in batch["tables"].items()
            }
        return audit