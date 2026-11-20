"""源端绑定接管校验接口：核对重叠区间、生成续读批次、确认交接与重启核验。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult
from app.services.source_takeover import SourceTakeoverService

router = APIRouter(prefix="/api/source_takeover", tags=["源端接管校验"])
service = SourceTakeoverService()


@router.get("/bindings")
def list_bindings(
    源端编号: str | None = Query(default=None),
    表名: str | None = Query(default=None),
    角色: str | None = Query(default=None),
) -> dict[str, Any]:
    """按来源、表名与角色列出接入端绑定，核对旧、新绑定是否属于同一张表。"""
    items = service.list_bindings(源端编号=源端编号, 表名=表名, 角色=角色)
    return {"items": items, "total": len(items)}


@router.get("/events")
def list_events(
    源端编号: str | None = Query(default=None),
    表名: str | None = Query(default=None),
    绑定编号: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    """列出两端事件，用于核对采集区间里的重叠变更。"""
    items = service.list_events(
        源端编号=源端编号, 表名=表名, 绑定编号=绑定编号, limit=limit)
    return {"items": items, "total": len(items)}


@router.get("/batches")
def list_batches(
    源端编号: str | None = Query(default=None),
    表名: str | None = Query(default=None),
    状态: str | None = Query(default=None),
) -> dict[str, Any]:
    """接管位点批次清单：续读时按这份清单实际读事件。"""
    items = service.list_batches(源端编号=源端编号, 表名=表名, 状态=状态)
    return {"items": items, "total": len(items)}


@router.get("/cursors")
def list_cursors() -> dict[str, Any]:
    """各来源、各表的续读位置；同名表分属不同来源时各自独立。"""
    items = service.list_cursors()
    return {"items": items, "total": len(items)}


@router.get("/check")
def check() -> dict[str, Any]:
    """交接清单回查：双方绑定、位点、已读区间和仍未确认的区间。"""
    return service.audit()


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int) -> dict[str, Any]:
    batch = service.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"接管批次 {batch_id} 不存在")
    return batch


@router.post("/prepare")
def prepare(payload: dict[str, Any]) -> ActionResult:
    """接管校验：按源端编号、表名和采集区间核对重叠事件，生成续读批次。"""
    batch, message = service.prepare(payload)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/batches/{batch_id}/resume")
def resume(batch_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """CDC 按批次续读事件，回写交付记录；重复的事件不会再交付一次。"""
    data, message = service.resume(batch_id, payload or {})
    if data is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=data)


@router.post("/batches/{batch_id}/confirm")
def confirm(batch_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """确认接管并推进续读位置；同一张表只允许一个确认推进，重试幂等。"""
    batch, message = service.confirm(batch_id, payload or {})
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/batches/{batch_id}/backfill")
def backfill(batch_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """旧端补齐尾段；未交付的尾段变更不会被当成已交接。"""
    batch, message = service.backfill_old_tail(batch_id, payload or {})
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/verify")
def verify(payload: dict[str, Any]) -> ActionResult:
    """核验批次与来源、表名是否匹配，防止借用其他表的确认。"""
    data, message = service.verify(payload)
    if data is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=data)


@router.post("/restart")
def restart() -> dict[str, Any]:
    """模拟重启：从交接清单恢复实际消费的变更与仍由旧绑定负责的区间。"""
    return service.restart()
