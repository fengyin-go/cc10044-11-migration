# 湖仓一体任务编排与数据治理平台

面向数据湖和数据仓库的ETL任务编排、分区管理、数据血缘和质量治理的工程平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py      通用查询/导出/登记/动作接口
│   ├── app/routers/migration_ops.py 迁移上线与血缘取数接口
│   ├── app/services/<key>.py        每个模块独立领域逻辑
│   ├── app/services/migration_registry.py  Hive→Iceberg 表分区映射登记表
│   └── app/store.py          内存数据仓库与示例数据
├── scripts/verify_migration_workflow.py  迁移上线运行手册端到端核验
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8018/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5198/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8018`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| ETL流水线 | `etl_pipeline` | 流水线 | 流水线名称、源端类型、目标端类型 |
| 分区管理 | `partition_manager` | 分区配置 | 配置名称、目标表、分区字段 |
| 数据质量规则 | `data_quality_rule` | 质量规则 | 规则名称、校验维度、目标表 |
| Schema变更管理 | `schema_change` | 变更记录 | 目标表、变更类型、变更内容 |
| 数据血缘 | `lineage_graph` | 血缘关系 | 目标表、来源表列表、转换逻辑 |
| 补数任务 | `backfill_task` | 补数任务 | 目标表、补数时间范围、补数状态 |
| 表归档管理 | `table_archive` | 归档策略 | 策略名称、目标表、归档条件 |
| 聚合表构建器 | `agg_builder` | 聚合配置 | 配置名称、源事实表、聚合维度 |
| 流水线监控 | `pipeline_monitor` | 监控记录 | 流水线名称、运行时间、执行状态 |
| 冷数据恢复 | `cold_restore` | 恢复任务 | 归档来源、恢复目标路径、恢复时间范围 |
| 维度表生成器 | `dimension_builder` | 维度配置 | 配置名称、来源表、维度键 |
| 数据保留策略 | `data_retention` | 保留规则 | 规则名称、目标数据集、保留天数 |
| 转换方案 | `transform_plan` | 转换步骤 | 方案名称、步骤序列、输入表 |
| 数据迁移任务 | `data_migration` | 迁移任务 | 任务名称、源存储、目标存储 |
| 增量合并引擎 | `delta_merge` | 合并任务 | 目标表、增量来源、合并键 |
| 快照生成器 | `snapshot_creator` | 快照任务 | 目标数据集、快照时间点、快照类型 |
| 湖仓审计 | `data_lake_audit` | 审计报告 | 审计范围、审计时间、表总数 |
| SLA策略 | `sla_policy` | SLA规则 | 规则名称、关联流水线、产出截止时间 |
| 清理作业 | `cleanup_job` | 清理作业 | 作业名称、目标路径、清理条件 |
| 目录同步 | `catalog_sync` | 同步任务 | 任务名称、源Catalog、目标Catalog |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## Hive→Iceberg 迁移上线

`data_migration` 任务 1（Hive到Iceberg订单迁移，批次 MB-2026-11-21-orders）登记了
`ods_orders` 与 `dwd_orders_clean` 的表分区映射：每行含 Hive/Iceberg 双侧地址、
实际行数、核验状态和 Iceberg 目标数据版本（snapshot id）。切换前取数走 Hive
存量读路径；组合报表 `rpt_orders_daily_combo` 沿血缘联合解析两张表。

| 接口 | 说明 |
| --- | --- |
| `GET /api/data_migration/{id}/partition-map` | 查看批次表分区映射与读路径状态 |
| `POST /api/data_migration/{id}/verify` `{table}` | 核验/重验单表，比对结果标清表和目标版本 |
| `POST /api/data_migration/{id}/backfill` `{table, partition}` | 补迁缺失或不符分区，补后待核验 |
| `POST /api/data_migration/{id}/switch` | 全批次核验通过后切换 Iceberg 读路径 |
| `POST /api/data_migration/{id}/rollback` | 回滚 Hive 存量读路径，保留 Iceberg 核验记录 |
| `GET /api/lineage_graph/fetch-address?table=...` | 单表血缘取数地址 |
| `GET /api/lineage_graph/joint-fetch-address?tables=...&report=...` | 联合取数地址，同一结果的输入来自一套完整可读数据 |

规约：漏一日或核验不符的表不能当作已迁完，未整表核验通过禁止切换；联合取数
时任一表未就绪则全部输入统一回退 Hive 存量分区。运行手册端到端核验：

```bash
python3 scripts/verify_migration_workflow.py
```

领域：数据工程
子方向：数仓与数据湖
