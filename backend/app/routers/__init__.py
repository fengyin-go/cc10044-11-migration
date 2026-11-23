"""业务路由汇总：迁移上线/血缘取数等静态路由必须先于通用资源路由注册。"""
from __future__ import annotations

from app.routers import migration_ops, resource

ROUTERS = [migration_ops, resource]
