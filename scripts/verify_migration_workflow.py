#!/usr/bin/env python3
"""Hive→Iceberg 订单迁移上线运行手册的端到端核验脚本。

场景：迁移批次 MB-2026-11-21-orders 含 ods_orders（已核验）和
dwd_orders_clean（缺 dt=2026-11-21 一个分区），两表共同提供组合报表
rpt_orders_daily_combo。脚本模拟值班员完整操作：

  1. 查看表分区映射（行数、分区、核验结果、目标数据版本）
  2. 沿单表血缘查询取数地址（未切换 → Hive 存量读路径）
  3. 沿组合报表血缘联合取数（两表地址同属一套完整可读数据）
  4. 切换被拦截（漏一日的表不能当作已迁完）
  5. 补迁缺失分区 → 重验 → 切换 Iceberg
  6. 切换后血缘查询返回核验通过的 Iceberg 地址
  7. 回滚：Hive 分区继续可读，Iceberg 核验记录保留
  8. 核对组合报表两张表的分区地址

用法：python3 scripts/verify_migration_workflow.py
脚本会自行在 8019 端口拉起后端服务，结束后自动关闭。
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:8019"
BACKEND_DIR = Path(__file__).resolve().parent.parent / "backend"
UVICORN = BACKEND_DIR / ".venv" / "bin" / "uvicorn"

FAILURES: list[str] = []


def req(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + (f"  -- {detail}" if not cond and detail else ""))
    if not cond:
        FAILURES.append(f"{name}: {detail}")


def engines_of(payload: dict) -> set[str]:
    return {t["engine"] for t in payload["tables"]}


def main() -> int:
    server = subprocess.Popen(
        [str(UVICORN), "app.main:app", "--host", "127.0.0.1", "--port", "8019"],
        cwd=BACKEND_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(60):
            try:
                status, health = req("GET", "/api/health")
                if status == 200 and health.get("ok"):
                    break
            except urllib.error.URLError:
                time.sleep(0.5)
        else:
            print("后端服务未能启动")
            return 1

        print("== 0. 通用资源路由未被新路由遮蔽 ==")
        status, listing = req("GET", "/api/data_migration")
        check("迁移任务列表可读", status == 200 and listing["total"] >= 3)
        task1 = next(t for t in listing["items"] if t["id"] == 1)
        check("迁移任务1处于迁移中", task1["status"] == "迁移中", task1["status"])
        status, lineage_list = req("GET", "/api/lineage_graph")
        check("血缘列表可读", status == 200 and lineage_list["total"] >= 5)
        status, detail = req("GET", "/api/lineage_graph/1")
        check("血缘详情按 id 可读", status == 200 and detail.get("目标表") == "dwd_orders_clean")

        print("== 1. 表分区映射登记（行数/分区/核验结果/目标版本） ==")
        status, pmap = req("GET", "/api/data_migration/1/partition-map")
        check("分区映射可读", status == 200 and pmap["ok"])
        check("读路径保持 Hive 存量", pmap["read_mode"] == "hive")
        tables = {t["table"]: t for t in pmap["tables"]}
        check("ods_orders 整表已核验", tables["ods_orders"]["fully_verified"] is True)
        check("dwd_orders_clean 未迁完", tables["dwd_orders_clean"]["fully_verified"] is False)
        check("dwd 缺 dt=2026-11-21",
              tables["dwd_orders_clean"]["incomplete_partitions"] == ["dt=2026-11-21"])
        ods_part = tables["ods_orders"]["partitions"][0]
        check("映射含双侧行数与目标版本",
              ods_part["hive_rows"] == ods_part["iceberg_rows"]
              and bool(ods_part["target_version"]))

        print("== 2. 值班员沿单表血缘查询取数地址（未切换） ==")
        status, addr_ods = req("GET", "/api/lineage_graph/fetch-address?table=ods_orders")
        check("ods_orders 走 Hive 存量读路径",
              addr_ods["engine"] == "hive" and len(addr_ods["addresses"]) == 4)
        check("ods_orders 地址为 Hive 分区",
              all(a["address"].startswith("hdfs://") for a in addr_ods["addresses"]))
        status, addr_dwd = req("GET", "/api/lineage_graph/fetch-address?table=dwd_orders_clean")
        check("dwd_orders_clean 走 Hive 存量读路径", addr_dwd["engine"] == "hive")

        print("== 3. 值班员沿组合报表血缘联合取数 ==")
        status, joint = req("GET", "/api/lineage_graph/joint-fetch-address?report=rpt_orders_daily_combo")
        check("组合报表联合取数一致", joint["ok"] and joint["consistent"] is True)
        check("联合结果两个输入同属 Hive 一套数据",
              joint["engine"] == "hive" and engines_of(joint) == {"hive"})
        status, joint2 = req("GET", "/api/lineage_graph/joint-fetch-address?tables=ods_orders,dwd_orders_clean")
        check("显式表清单联合取数同样回 Hive", joint2["engine"] == "hive")

        print("== 4. 漏一日未补迁前禁止切换 ==")
        status, switched = req("POST", "/api/data_migration/1/switch")
        check("切换被拦截", switched["ok"] is False)
        check("拦截原因指向 dwd_orders_clean",
              "dwd_orders_clean" in switched.get("not_ready_tables", []))
        status, verified = req("POST", "/api/data_migration/1/verify", {"table": "dwd_orders_clean"})
        check("核验结果标清表和目标数据版本字段",
              all("table" in c and "target_version" in c for c in verified["checks"]))
        missing = [c for c in verified["checks"] if c["partition"] == "dt=2026-11-21"][0]
        check("漏一日分区判为缺失且不算迁完",
              missing["verify_status"] == "缺失" and verified["table_migrated"] is False)

        print("== 5. 补迁 → 重验 → 切换 ==")
        status, backfilled = req("POST", "/api/data_migration/1/backfill",
                                 {"table": "dwd_orders_clean", "partition": "dt=2026-11-21"})
        check("补迁完成并生成新目标版本",
              backfilled["ok"] and backfilled["partition"]["verify_status"] == "待核验"
              and bool(backfilled["partition"]["target_version"]))
        new_version = backfilled["partition"]["target_version"]
        status, switched = req("POST", "/api/data_migration/1/switch")
        check("补迁未重验仍禁止切换", switched["ok"] is False)
        status, verified = req("POST", "/api/data_migration/1/verify", {"table": "dwd_orders_clean"})
        check("重验后整表迁完", verified["ok"] is True and verified["table_migrated"] is True)
        check("重验沿用补迁产生的目标版本",
              verified["target_versions"]["dt=2026-11-21"] == new_version)
        status, task1 = req("GET", "/api/data_migration/1")
        check("全批次核验通过后任务置为已校验", task1["status"] == "已校验", task1["status"])
        status, switched = req("POST", "/api/data_migration/1/switch")
        check("切换上线成功", switched["ok"] is True and switched["read_mode"] == "iceberg")

        print("== 6. 上线后血缘查询返回核验通过的 Iceberg 地址 ==")
        status, addr_ods = req("GET", "/api/lineage_graph/fetch-address?table=ods_orders")
        check("ods_orders 单表走 Iceberg",
              addr_ods["engine"] == "iceberg"
              and all(a["address"].startswith("s3://") and a["target_version"]
                      for a in addr_ods["addresses"]))
        status, joint = req("GET", "/api/lineage_graph/joint-fetch-address?report=rpt_orders_daily_combo")
        check("组合报表两个输入同属 Iceberg 一套数据",
              joint["engine"] == "iceberg" and engines_of(joint) == {"iceberg"})
        versions = {a["target_version"] for t in joint["tables"] for a in t["addresses"]}
        check("联合结果每个地址都带目标数据版本", len(versions) == 8 and None not in versions)

        print("== 7. 回滚：Hive 可读，Iceberg 核验记录保留 ==")
        status, rolled = req("POST", "/api/data_migration/1/rollback")
        check("回滚成功", rolled["ok"] is True and rolled["read_mode"] == "hive")
        check("Iceberg 核验记录全部保留", rolled["retained_iceberg_verifications"] == 8)
        status, pmap = req("GET", "/api/data_migration/1/partition-map")
        kept = [p for t in pmap["tables"] for p in t["partitions"]
                if p["verify_status"] == "已核验" and p["target_version"]]
        check("分区映射中 8 条核验记录仍在", len(kept) == 8)
        actions = [h["action"] for h in pmap["switch_history"]]
        check("切换与回滚均留痕", actions == ["切换", "回滚"], str(actions))

        print("== 8. 核对组合报表两张表的分区地址 ==")
        status, joint = req("GET", "/api/lineage_graph/joint-fetch-address?report=rpt_orders_daily_combo")
        check("回滚后联合取数回到 Hive 一套数据",
              joint["engine"] == "hive" and engines_of(joint) == {"hive"})
        for table_addr in joint["tables"]:
            parts = {a["partition"]: a["address"] for a in table_addr["addresses"]}
            expected = {
                f"dt=2026-11-{day}": f"hdfs://nn-prod:8020/warehouse/orders/{table_addr['table']}/dt=2026-11-{day}"
                for day in ("18", "19", "20", "21")
            }
            check(f"{table_addr['table']} 四个分区地址为原 Hive 分区", parts == expected, str(parts))
    finally:
        server.terminate()
        server.wait(timeout=10)

    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项未通过：")
        for item in FAILURES:
            print(" -", item)
        return 1
    print("全部核验通过：迁移、补迁、重验、切换、回滚与血缘取数地址均符合规约。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
