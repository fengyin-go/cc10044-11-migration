"""数据血缘领域服务：管理表级和字段级的数据血缘图、上下游追踪和断裂检测。

数据链路位置：元数据层，维护数据从源到目标的完整血缘关系图。"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.migration_registry import migration_registry
from app.store import store


class LineageGraphService(BaseModuleService):
    MODULE = "lineage_graph"
    ENTITY = "血缘关系"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["目标表", "来源表列表", "转换逻辑", "中间层级", "血缘深度"]
    REQUIRED_FIELDS = ["目标表", "来源表列表", "转换逻辑"]
    STATUS_ORDER = ["已建立", "待验证", "已断裂"]
    ACTION_RULES = {"刷新血缘": "已建立", "标记断裂": "已断裂"}
    NEGATIVE_ACTIONS = []

    # ---- 迁移取数地址解析 ---------------------------------------------

    def fetch_address(self, table: str) -> dict[str, Any]:
        """单表取数地址：按经过核验的表分区映射解析。

        迁移上线（读路径切到 Iceberg）后返回核验通过的 Iceberg 分区地址；
        未切换或整表未核验通过时返回 Hive 存量分区地址，保证旧库读路径可用。
        """
        batch = migration_registry.batch_for_table(table)
        if batch is None:
            return {
                "ok": False,
                "table": table,
                "mapped": False,
                "message": f"表 {table} 未登记迁移分区映射，按原生路径取数",
            }
        resolved = migration_registry.resolve_table_address(batch, table)
        return {
            "ok": True,
            "table": table,
            "batch_id": batch["batch_id"],
            "read_mode": batch["read_mode"],
            **resolved,
        }

    def joint_fetch_address(
        self, tables: list[str] | None = None, report: str | None = None
    ) -> dict[str, Any]:
        """联合取数地址：同一结果的全部输入地址来自一套完整可读的数据。

        可以显式传表清单，也可以传组合报表名（沿该报表已建立的血缘解析
        上游表）。任一输入表未就绪时，全部输入统一回退 Hive 存量分区，
        不允许同一结果混用两个引擎的地址。
        """
        if report:
            entry = next(
                (e for e in store.rows(self.MODULE)
                 if e.get("目标表") == report and e.get("status") == "已建立"),
                None,
            )
            if entry is None:
                return {"ok": False, "message": f"组合报表 {report} 没有已建立的血缘关系"}
            tables = [s.strip() for s in entry.get("来源表列表", "").split(",") if s.strip()]
        tables = [t for t in (tables or []) if t]
        if not tables:
            return {"ok": False, "message": "请提供至少一张取数表或一个组合报表名"}

        resolved_batches: dict[int, dict[str, Any]] = {}
        unmapped: list[str] = []
        for table in tables:
            batch = migration_registry.batch_for_table(table)
            if batch is None:
                unmapped.append(table)
            else:
                resolved_batches[id(batch)] = batch
        if unmapped:
            return {
                "ok": False,
                "message": f"表 {','.join(unmapped)} 未登记迁移分区映射，无法联合解析",
                "unmapped_tables": unmapped,
            }

        if len(resolved_batches) > 1:
            return {"ok": False, "message": "输入表分属不同迁移批次，无法保证同一套数据"}
        batch = next(iter(resolved_batches.values()))
        joint = migration_registry.resolve_joint_address(batch, tables)
        return {
            "ok": True,
            "report": report,
            "tables": joint["tables"],
            "engine": joint["engine"],
            "consistent": joint["consistent"],
            "read_mode": batch["read_mode"],
            "batch_id": batch["batch_id"],
            "message": joint["note"],
        }

    def trace_upstream(self, target_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从目标表向上追溯所有上游来源表和转换链路。

        返回完整的上游依赖树，每个节点包含表名、所在层级和转换逻辑。
        用于变更影响分析。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(target_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                if entry.get("目标表") == current and entry.get("status") == "已建立":
                    sources = entry.get("来源表列表", "").split(",")
                    node = {
                        "table": current,
                        "depth": depth,
                        "sources": [s.strip() for s in sources if s.strip()],
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                    }
                    tree.append(node)
                    for src in node["sources"]:
                        if src not in visited:
                            queue.append((src, depth + 1))

        return {
            "target_table": target_table,
            "max_depth": max_depth,
            "total_nodes": len(tree),
            "upstream_tree": tree,
            "leaf_tables": [n["table"] for n in tree if not n["sources"]],
        }

    def trace_downstream(self, source_table: str, max_depth: int = 5) -> dict[str, Any]:
        """从来源表向下追溯所有下游受影响表和转换链路。

        用于评估某个表变更时的影响范围，联动质量规则模块通知下游。
        """
        all_entries = store.rows(self.MODULE)
        visited: set[str] = set()
        tree: list[dict[str, Any]] = []
        queue = [(source_table, 0)]
        while queue and len(visited) < 50:
            current, depth = queue.pop(0)
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            for entry in all_entries:
                sources = entry.get("来源表列表", "").split(",")
                source_set = {s.strip() for s in sources if s.strip()}
                if current in source_set and entry.get("status") == "已建立":
                    target = entry.get("目标表", "")
                    node = {
                        "table": target,
                        "depth": depth + 1,
                        "upstream_of": current,
                        "transform": entry.get("转换逻辑"),
                        "lineage_id": entry.get("id"),
                    }
                    tree.append(node)
                    if target not in visited:
                        queue.append((target, depth + 1))

        return {
            "source_table": source_table,
            "max_depth": max_depth,
            "total_affected": len(tree),
            "downstream_tree": tree,
            "end_consumers": [n["table"] for n in tree if not any(
                e.get("来源表列表", "").find(n["table"]) >= 0
                for e in all_entries if e.get("status") == "已建立"
            )],
        }

    def detect_broken_lineage(self) -> dict[str, Any]:
        """扫描并检测断裂的血缘关系。

        当上游来源表已被迁移、归档或删除时，其下游血缘关系变为断裂状态。
        检测结果包含受影响的目标表和对应的修复建议。
        """
        all_entries = store.rows(self.MODULE)
        broken: list[dict[str, Any]] = []
        for entry in all_entries:
            if entry.get("status") == "已断裂":
                broken.append({
                    "lineage_id": entry.get("id"),
                    "target_table": entry.get("目标表"),
                    "sources": entry.get("来源表列表"),
                    "needs_revalidation": True,
                })

        table_archive_rows = store.rows("table_archive")
        migration_rows = store.rows("data_migration")

        for entry in all_entries:
            if entry.get("status") != "已建立":
                continue
            sources = entry.get("来源表列表", "").split(",")
            for src in sources:
                src = src.strip()
                if not src:
                    continue
                archived = any(
                    a.get("目标表") == src and a.get("status") == "已启用"
                    for a in table_archive_rows
                )
                if archived:
                    entry["status"] = "已断裂"
                    broken.append({
                        "lineage_id": entry.get("id"),
                        "target_table": entry.get("目标表"),
                        "source": src,
                        "reason": "来源表已归档",
                        "needs_revalidation": True,
                    })

        return {
            "broken_count": len(broken),
            "broken_lineages": broken,
            "suggestion": "请重新校验断裂血缘并更新目标表关联",
        }

    def build_inheritance_chain(self, table_name: str) -> dict[str, Any]:
        """为指定表构建完整的继承链（上下游合起来）。

        从该表出发，同时向上追溯祖先表和向下追溯后代表，
        生成完整的数据流转全景图。
        """
        upstream = self.trace_upstream(table_name)
        downstream = self.trace_downstream(table_name)
        return {
            "table": table_name,
            "upstream_depth": upstream["total_nodes"],
            "downstream_affected": downstream["total_affected"],
            "upstream_chain": upstream["upstream_tree"],
            "downstream_chain": downstream["downstream_tree"],
            "orphan_risk": upstream["total_nodes"] == 0 and downstream["total_affected"] > 0,
        }

    def impact_analysis(self, entry_id: int) -> dict[str, Any]:
        """变更影响分析：当某个血缘条目的转换逻辑发生变更时，
        评估对下游的级联影响范围和受影响的数据质量规则。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "血缘条目不存在"}

        target_table = entry.get("目标表", "")
        downstream = self.trace_downstream(target_table)

        quality_rows = store.rows("data_quality_rule")
        affected_quality_rules = [
            q for q in quality_rows
            if q.get("目标表") in [n["table"] for n in downstream["downstream_tree"]]
        ]

        return {
            "lineage_id": entry_id,
            "source_table": target_table,
            "change_type": "transform_logic_changed",
            "direct_downstream_tables": len(downstream["downstream_tree"]),
            "cascaded_tables": downstream["total_affected"],
            "affected_quality_rule_count": len(affected_quality_rules),
            "affected_quality_rules": [
                {"rule_id": r["id"], "rule_name": r.get("规则名称"), "target_table": r.get("目标表")}
                for r in affected_quality_rules[:10]
            ],
            "recommendation": "建议在变更后立即执行受影响的质量规则校验",
        }