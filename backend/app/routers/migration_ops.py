"""迁移上线与血缘取数接口：Hive→Iceberg 批次的核验、补迁、切换、回滚，
以及按经过核验的表分区映射解析取数地址（单表与联合取数）。

这些路由路径是静态或三段式的，必须在通用资源路由之前注册，
避免被 ``/api/{module}/{entry_id}`` 抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services import service_for
from app.services.data_migration import DataMigrationService
from app.services.lineage_graph import LineageGraphService

router = APIRouter(prefix="/api", tags=["迁移上线与血缘取数"])


class TablePayload(BaseModel):
    table: str


class BackfillPayload(BaseModel):
    table: str
    partition: str


def _migration_service() -> DataMigrationService:
    service = service_for("data_migration")
    if not isinstance(service, DataMigrationService):  # pragma: no cover
        raise HTTPException(status_code=500, detail="data_migration 服务未就绪")
    return service


def _lineage_service() -> LineageGraphService:
    service = service_for("lineage_graph")
    if not isinstance(service, LineageGraphService):  # pragma: no cover
        raise HTTPException(status_code=500, detail="lineage_graph 服务未就绪")
    return service


def _ensure_task(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("message") == "迁移任务不存在":
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/data_migration/{entry_id}/partition-map")
def partition_map(entry_id: int) -> dict[str, Any]:
    """查看迁移批次登记的表分区映射（行数、分区、核验结果、目标版本）。"""
    return _ensure_task(_migration_service().partition_map(entry_id))


@router.post("/data_migration/{entry_id}/verify")
def verify_table(entry_id: int, payload: TablePayload) -> dict[str, Any]:
    """核验（或重验）批次内单表，比对结果标清表和目标数据版本。"""
    return _ensure_task(_migration_service().verify_table(entry_id, payload.table.strip()))


@router.post("/data_migration/{entry_id}/backfill")
def backfill_partition(entry_id: int, payload: BackfillPayload) -> dict[str, Any]:
    """补迁缺失或核验不符的分区，补迁后需重新核验。"""
    return _ensure_task(_migration_service().backfill_partition(
        entry_id, payload.table.strip(), payload.partition.strip()))


@router.post("/data_migration/{entry_id}/switch")
def switch_read_path(entry_id: int) -> dict[str, Any]:
    """全批次核验通过后，把读路径切换到 Iceberg。"""
    return _ensure_task(_migration_service().switch_read_path(entry_id))


@router.post("/data_migration/{entry_id}/rollback")
def rollback_read_path(entry_id: int) -> dict[str, Any]:
    """回滚读路径到 Hive 存量分区，保留 Iceberg 核验记录。"""
    return _ensure_task(_migration_service().rollback_read_path(entry_id))


@router.get("/lineage_graph/fetch-address")
def fetch_address(table: str = Query(...)) -> dict[str, Any]:
    """单表血缘取数地址：按经过核验的表分区映射解析。"""
    return _lineage_service().fetch_address(table.strip())


@router.get("/lineage_graph/joint-fetch-address")
def joint_fetch_address(
    tables: str | None = Query(default=None),
    report: str | None = Query(default=None),
) -> dict[str, Any]:
    """联合取数地址：同一结果的全部输入来自一套完整可读的数据。"""
    table_list = [t.strip() for t in (tables or "").split(",") if t.strip()]
    return _lineage_service().joint_fetch_address(
        tables=table_list or None, report=report.strip() if report else None)
