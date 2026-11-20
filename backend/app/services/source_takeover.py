"""源端绑定接管校验：换绑时核对两端事件，生成 CDC 续读用的接管位点批次。

核心约束（对应换绑交接事故的教训）：

* 接管范围只由 ``源端编号 + 表名 + 采集区间`` 判定，绑定条件必须包含来源；
  同名但不同来源的表的位点、确认和消费记录互不借用。
* 换绑期间旧、新两端可能读到同一批变更（同一个 ``变更编号`` 在两端位点
  不同）。接管批次保留双方各自的来源、绑定和区间，交付时按变更编号去重。
* 整表接管时点只是新绑定声明读到的尾位点，不等于已核对区间的终点；续读
  从已核对区间的起点开始，已核对但尚未被 CDC 消费的变更不能被尾位点跳过。
* 旧连接在交出尾位点前断开时，旧端未确认区间仍归旧绑定负责，不能记成已
  交接；只有补齐尾段或显式交出尾位点后才能闭合。
* 确认以 ``源端编号 + 表名`` 为粒度做乐观并发控制：同一张表只允许一个确认
  把位点向前推进，重复确认是幂等的重试，不会把业务变更再交付一次。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

BINDINGS = "_takeover_bindings"
EVENTS = "_takeover_events"
CURSORS = "_cdc_cursors"
DELIVERED = "_cdc_delivered"
BATCHES = "_takeover_batches"

REQUIRED_FIELDS = ["源端编号", "表名"]
TERMINAL_STATUS = "已交接"

_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _interval(payload: dict[str, Any]) -> tuple[int | None, int | None]:
    """采集区间同时兼容 {开始位点,结束位点} 与 {起始位点,截止位点} 两种写法。"""
    raw = payload.get("采集区间")
    start = end = None
    if isinstance(raw, dict):
        start = _as_int(raw.get("开始位点") or raw.get("起始位点"))
        end = _as_int(raw.get("结束位点") or raw.get("截止位点"))
    start = start if start is not None else _as_int(payload.get("起始位点"))
    end = end if end is not None else _as_int(payload.get("结束位点") or payload.get("截止位点"))
    return start, end


def _in_interval(position: int, start: int | None, end: int | None) -> bool:
    if start is not None and position < start:
        return False
    if end is not None and position > end:
        return False
    return True


def _span(positions: list[int]) -> dict[str, int]:
    return {"起始位点": min(positions), "结束位点": max(positions)}


def _delivery_key(source: str, table: str, change: str) -> tuple[str, str, str]:
    return (source, table, change)


class SourceTakeoverService:
    # ------------------------------------------------------------------
    # 只读查询
    # ------------------------------------------------------------------
    def list_bindings(
        self, *, 源端编号: str | None = None, 表名: str | None = None, 角色: str | None = None
    ) -> list[dict[str, Any]]:
        rows = store.rows(BINDINGS)
        if 源端编号:
            rows = [row for row in rows if row.get("源端编号") == 源端编号]
        if 表名:
            rows = [row for row in rows if row.get("表名") == 表名]
        if 角色:
            rows = [row for row in rows if row.get("角色") == 角色]
        return [dict(row) for row in rows]

    def list_events(
        self,
        *,
        源端编号: str | None = None,
        表名: str | None = None,
        绑定编号: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = store.rows(EVENTS)
        if 源端编号:
            rows = [row for row in rows if row.get("源端编号") == 源端编号]
        if 表名:
            rows = [row for row in rows if row.get("表名") == 表名]
        if 绑定编号:
            rows = [row for row in rows if row.get("绑定编号") == 绑定编号]
        ordered = sorted(rows, key=lambda row: (int(row.get("序号", 0)), int(row.get("位点", 0))))
        return [dict(row) for row in ordered[:limit]]

    def list_batches(
        self, *, 源端编号: str | None = None, 表名: str | None = None, 状态: str | None = None
    ) -> list[dict[str, Any]]:
        rows = store.rows(BATCHES)
        if 源端编号:
            rows = [row for row in rows if row.get("源端编号") == 源端编号]
        if 表名:
            rows = [row for row in rows if row.get("表名") == 表名]
        if 状态:
            rows = [row for row in rows if row.get("状态") == 状态]
        return [self._batch_view(row) for row in rows]

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        row = self._find_batch(batch_id)
        return self._batch_view(row) if row else None

    def list_cursors(self) -> list[dict[str, Any]]:
        return [self._cursor_view(row["源端编号"], row["表名"]) for row in store.rows(CURSORS)]

    def get_cursor(self, source: str, table: str) -> dict[str, Any] | None:
        for row in store.rows(CURSORS):
            if row.get("源端编号") == source and row.get("表名") == table:
                return row
        return None

    # ------------------------------------------------------------------
    # 接管校验：核对重叠区间，生成续读批次
    # ------------------------------------------------------------------
    def prepare(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        source = _text(values.get("源端编号"))
        table = _text(values.get("表名"))
        missing = [field for field, value in zip(REQUIRED_FIELDS, (source, table)) if not value]
        if missing:
            return None, f"缺少必填字段：{'、'.join(missing)}"

        start, end = _interval(values)
        if start is not None and end is not None and start > end:
            return None, "采集区间的开始位点不能大于结束位点"

        with _LOCK:
            bindings = self._scope_bindings(source, table, values)
            if isinstance(bindings, str):
                return None, bindings
            old_binding, new_binding = bindings

            old_events = self._scope_events(old_binding["绑定编号"], source, table, start, end)
            new_events = self._scope_events(new_binding["绑定编号"], source, table, start, end)
            verified = self._verified_changes(old_events, new_events)
            if not verified:
                return None, (
                    f"源端 {source} 的表 {table} 在采集区间内没有两端都核对到的变更，"
                    "无法确认接管范围"
                )

            # 同一来源、同一张表只保留一个待推进的续读位置。
            existing = self._active_batch(source, table)
            if existing is not None:
                return self._batch_view(existing), "该来源与表已有待完成的接管批次，已复用"

            old_span = _span([int(row["位点"]) for row in old_events])
            new_span = _span([int(row["位点"]) for row in new_events])
            verified_start = min(int(item["新端位点"]) for item in verified)
            verified_end = max(int(item["新端位点"]) for item in verified)

            # 整表接管时点取新绑定声明读到的尾位点，它不能替代已核对区间终点。
            takeover_point = max(
                int(new_binding.get("位点") or new_span["结束位点"]), new_span["结束位点"]
            )
            unconfirmed = self._unconfirmed_intervals(old_binding, old_events, verified)

            batch = {
                "id": max((int(row.get("id", 0)) for row in store.rows(BATCHES)), default=0) + 1,
                "批次编号": self._next_batch_code(),
                "源端编号": source,
                "表名": table,
                "采集区间": {
                    "起始位点": start if start is not None else min(
                        old_span["起始位点"], new_span["起始位点"]),
                    "结束位点": end if end is not None else max(
                        old_span["结束位点"], new_span["结束位点"]),
                },
                "旧绑定": self._binding_snapshot(old_binding),
                "新绑定": self._binding_snapshot(new_binding),
                "旧端区间": old_span,
                "新端区间": new_span,
                "已核对变更": verified,
                "已核对区间": {
                    "起始位点": verified_start,
                    "结束位点": verified_end,
                    "变更编号": [item["变更编号"] for item in verified],
                },
                # 续读从已核对区间起点开始，避免尾位点跳过尚未消费的已核对变更。
                "续读起点": verified_start,
                "续读上限": takeover_point,
                "整表接管时点": takeover_point,
                "未确认区间": unconfirmed,
                "状态": "待旧端补齐" if unconfirmed else "待续读",
                "续读状态": "未续读",
                "已交付变更": [],
                "已交付位点": None,
                "已读区间": None,
                "确认位点": None,
                "确认时间": None,
                "确认绑定": None,
                "已回写变更": [],
                "旧端尾位点": None,
                "补齐记录": [],
                "创建时间": _now(),
            }
            store.rows(BATCHES).append(batch)
            return self._batch_view(batch), "接管校验完成，已生成续读位点批次"

    # ------------------------------------------------------------------
    # CDC 续读：按批次清单读事件，回写交付记录
    # ------------------------------------------------------------------
    def resume(
        self, batch_id: int, values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        with _LOCK:
            batch = self._find_batch(batch_id)
            if batch is None:
                return None, f"接管批次 {batch_id} 不存在"

            mismatch = self._key_mismatch(batch, values)
            if mismatch:
                return None, mismatch

            cursor = self.get_cursor(batch["源端编号"], batch["表名"])
            if cursor is None:
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 没有登记续读位置"
                )

            expected = _as_int(values.get("期望位点"))
            if expected is not None and expected != int(cursor.get("确认位点") or 0):
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 的确认位点已变为 "
                    f"{cursor.get('确认位点')}，本次续读不能借用旧位点"
                )

            owner = cursor.get("确认批次")
            if (
                owner not in (None, batch["批次编号"])
                and int(cursor.get("确认位点") or 0) >= int(batch["续读上限"])
            ):
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 的续读位置已由批次 "
                    f"{owner} 推进，本批次不能借用"
                )

            candidates, duplicates = self._candidates(batch, cursor)
            delivered_at = _now()
            for event in candidates:
                self._record_delivery(batch, cursor, event, delivered_at)

            if candidates:
                batch["续读状态"] = "已续读"
                if batch["状态"] == "待续读":
                    batch["状态"] = "续读中"
                cursor["数据游标"] = max(
                    int(cursor.get("数据游标") or 0),
                    max(int(item["位点"]) for item in candidates),
                )

            payload = {
                "批次编号": batch["批次编号"],
                "源端编号": batch["源端编号"],
                "表名": batch["表名"],
                "采集区间": batch["采集区间"],
                "续读起点": batch["续读起点"],
                "续读上限": batch["续读上限"],
                "事件": candidates,
                "跳过重复": duplicates,
                "确认位点": cursor.get("确认位点"),
                "数据游标": cursor.get("数据游标"),
            }
            if candidates:
                message = f"续读到 {len(candidates)} 笔变更"
            elif batch.get("确认位点") is not None:
                message = "该批次区间已确认，未重复交付变更"
            else:
                message = "没有可续读的新变更"
            if duplicates:
                message += f"，跳过 {len(duplicates)} 笔已交付变更"
            return payload, message

    # ------------------------------------------------------------------
    # 接管确认：幂等推进一张表的续读位置
    # ------------------------------------------------------------------
    def confirm(
        self, batch_id: int, values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        with _LOCK:
            batch = self._find_batch(batch_id)
            if batch is None:
                return None, f"接管批次 {batch_id} 不存在"

            mismatch = self._key_mismatch(batch, values)
            if mismatch:
                return None, mismatch

            cursor = self.get_cursor(batch["源端编号"], batch["表名"])
            if cursor is None:
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 没有登记续读位置"
                )

            # 重复确认是重试，不是第二次交接：不再推进位点。
            if batch.get("确认位点") is not None:
                return self._batch_view(batch), "接管批次已确认，重复确认未推进位点"

            expected = _as_int(values.get("期望位点"))
            if expected is not None and expected != int(cursor.get("确认位点") or 0):
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 的确认位点已变为 "
                    f"{cursor.get('确认位点')}，本批次不能并发推进"
                )

            owner = cursor.get("确认批次")
            if (
                owner not in (None, batch["批次编号"])
                and int(cursor.get("确认位点") or 0) >= int(batch["续读上限"])
            ):
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 的续读位置已由批次 "
                    f"{owner} 推进，本批次不能借用"
                )

            # 调用方可以回报实际消费的变更；未回报时以续读时登记的交付记录为准。
            reported = values.get("已消费变更") or []
            if reported:
                events, unknown = self._events_for_changes(batch, reported)
                if unknown:
                    return None, (
                        f"回报的变更 {'、'.join(unknown)} 不属于源端 {batch['源端编号']} "
                        f"表 {batch['表名']} 的本次接管区间，不能计入确认"
                    )
                for event in events:
                    self._record_delivery(batch, cursor, event, _now())

            if not batch["已交付变更"]:
                return None, "该批次尚未续读，没有可确认的已读区间"

            coverage = self._coverage(batch)
            if not coverage["完整"]:
                return None, (
                    f"源端 {batch['源端编号']} 表 {batch['表名']} 的续读区间尚未完整交付，"
                    f"缺少变更 {'、'.join(coverage['缺少'])}"
                )

            confirmed_at = _now()
            confirm_point = int(batch["续读上限"])
            batch["确认位点"] = confirm_point
            batch["确认时间"] = confirmed_at
            batch["确认绑定"] = batch["新绑定"]["绑定编号"]
            batch["已读区间"] = {
                "起始位点": batch["续读起点"],
                "结束位点": confirm_point,
                "变更编号": [item["变更编号"] for item in batch["已交付变更"]],
            }
            batch["已回写变更"] = [dict(item) for item in batch["已交付变更"]]

            cursor["确认位点"] = confirm_point
            cursor["数据游标"] = max(int(cursor.get("数据游标") or 0), confirm_point)
            cursor["确认批次"] = batch["批次编号"]
            cursor["确认时间"] = confirmed_at
            cursor["绑定"] = batch["新绑定"]["绑定编号"]
            consumed = list(cursor.get("已消费变更") or [])
            for item in batch["已交付变更"]:
                if item["变更编号"] not in consumed:
                    consumed.append(item["变更编号"])
            cursor["已消费变更"] = consumed

            # 未确认区间不能当成已交接：旧绑定没补齐就仍停在待补齐。
            batch["状态"] = "待旧端补齐" if batch["未确认区间"] else TERMINAL_STATUS
            message = f"接管确认完成，续读位置推进到 {confirm_point}"
            if batch["未确认区间"]:
                message += "；旧端未确认区间仍由旧绑定负责"
            return self._batch_view(batch), message

    # ------------------------------------------------------------------
    # 旧端尾段补齐 / 重启核验
    # ------------------------------------------------------------------
    def backfill_old_tail(
        self, batch_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        with _LOCK:
            batch = self._find_batch(batch_id)
            if batch is None:
                return None, f"接管批次 {batch_id} 不存在"

            tail = _as_int(values.get("尾位点") if "尾位点" in values else values.get("结束位点"))
            if tail is None:
                return None, "缺少必填字段：尾位点"

            old_binding = batch["旧绑定"]
            supplied = _text(values.get("绑定编号") or values.get("源端编号"))
            if supplied and supplied not in (old_binding["绑定编号"], batch["源端编号"]):
                return None, (
                    f"绑定 {supplied} 不是源端 {batch['源端编号']} 表 {batch['表名']} 的旧绑定，"
                    "不能补齐尾段"
                )
            if tail < int(old_binding.get("位点") or 0):
                return None, (
                    f"旧端尾位点 {tail} 小于该绑定已声明的位点 {old_binding.get('位点')}"
                )

            verified_old_end = max(
                (int(item["旧端位点"]) for item in batch["已核对变更"]), default=None
            )
            if verified_old_end is None or tail <= verified_old_end:
                # 尾位点没有超出已核对区间，但新端尾段里尚未被 CDC 消费的变更
                # 依然不能当成已交接。
                coverage = self._coverage(batch)
                if not coverage["完整"]:
                    batch["旧端尾位点"] = tail
                    batch["未确认区间"] = [{
                        "绑定编号": old_binding["绑定编号"],
                        "源端编号": batch["源端编号"],
                        "表名": batch["表名"],
                        "起始位点": verified_old_end + 1,
                        "结束位点": tail,
                        "原因": f"续读区间尚未完整交付：{'、'.join(coverage['缺少'])}",
                    }]
                    return self._batch_view(batch), (
                        f"交接尚未闭合，仍有未交付变更：{'、'.join(coverage['缺少'])}"
                    )
                return self._close_old_tail(
                    batch, tail, "尾位点未超出已核对区间，未确认区间按已核对范围处理"
                )

            # 旧端尾段里只在新端出现的变更，必须已经由 CDC 交付才能闭合。
            delivered = {
                item["变更编号"]
                for item in store.rows(DELIVERED)
                if item.get("源端编号") == batch["源端编号"]
                and item.get("表名") == batch["表名"]
            }
            old_changes = {
                row.get("变更编号")
                for row in store.rows(EVENTS)
                if row.get("绑定编号") == old_binding["绑定编号"]
                and row.get("源端编号") == batch["源端编号"]
                and row.get("表名") == batch["表名"]
            }
            missing = sorted({
                str(row.get("变更编号"))
                for row in store.rows(EVENTS)
                if row.get("绑定编号") == batch["新绑定"]["绑定编号"]
                and row.get("源端编号") == batch["源端编号"]
                and row.get("表名") == batch["表名"]
                and int(row.get("序号", 0)) > self._last_verified_sequence(batch)
                and row.get("变更编号") not in old_changes
                and row.get("变更编号") not in delivered
            })
            if missing:
                batch["旧端尾位点"] = tail
                batch["未确认区间"] = [{
                    "绑定编号": old_binding["绑定编号"],
                    "源端编号": batch["源端编号"],
                    "表名": batch["表名"],
                    "起始位点": verified_old_end + 1,
                    "结束位点": tail,
                    "原因": f"旧端尾段仍有未交付变更：{'、'.join(missing)}",
                }]
                return self._batch_view(batch), (
                    f"旧端尾段未补齐，仍有未确认变更：{'、'.join(missing)}"
                )
            return self._close_old_tail(batch, tail, "尾段变更已全部交付")

    def restart(self) -> dict[str, Any]:
        """模拟重启后从交接清单重新核验：不改动已持久化的批次与消费记录。"""
        with _LOCK:
            problems: list[str] = []
            for batch in store.rows(BATCHES):
                has_unconfirmed = bool(batch.get("未确认区间"))
                if batch.get("确认位点") is not None:
                    batch["状态"] = "待旧端补齐" if has_unconfirmed else TERMINAL_STATUS
                elif batch.get("已交付变更"):
                    batch["状态"] = "待旧端补齐" if has_unconfirmed else "续读中"
                else:
                    batch["状态"] = "待旧端补齐" if has_unconfirmed else "待续读"

            keys = sorted({
                (row.get("源端编号"), row.get("表名")) for row in store.rows(BATCHES)
            } | {(row.get("源端编号"), row.get("表名")) for row in store.rows(CURSORS)})
            for source, table in keys:
                cursor = self.get_cursor(source, table)
                if cursor is None:
                    problems.append(f"源端 {source} 表 {table} 缺少续读位置登记")
                    continue
                seen: set[str] = set()
                for item in store.rows(DELIVERED):
                    if item.get("源端编号") != source or item.get("表名") != table:
                        continue
                    if item["变更编号"] in seen:
                        problems.append(
                            f"源端 {source} 表 {table} 的变更 {item['变更编号']} 被重复交付"
                        )
                    seen.add(item["变更编号"])
                if int(cursor.get("数据游标") or 0) < int(cursor.get("确认位点") or 0):
                    problems.append(f"源端 {source} 表 {table} 的数据游标落后于确认位点")
                scope_changes = {
                    str(row.get("变更编号"))
                    for row in store.rows(EVENTS)
                    if row.get("源端编号") == source and row.get("表名") == table
                }
                borrowed = [c for c in cursor.get("已消费变更", []) if c not in scope_changes]
                if borrowed:
                    problems.append(
                        f"源端 {source} 表 {table} 借用了其他来源的消费确认："
                        f"{'、'.join(map(str, borrowed))}"
                    )

            return {
                "重启时间": _now(),
                "批次": self.list_batches(),
                "各表": [self._cursor_view(source, table) for source, table in keys],
                "一致性": {"正常": not problems, "问题": problems},
            }

    def audit(self) -> dict[str, Any]:
        return self.restart()

    def verify(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """核验批次能否用于指定来源 + 表的续读，防止借用其他表的确认。"""
        batch_id = _as_int(values.get("批次编号"))
        batch = self._find_batch(batch_id) if batch_id is not None else None
        if batch is None:
            return None, f"接管批次 {values.get('批次编号')} 不存在"
        source = _text(values.get("源端编号"))
        table = _text(values.get("表名"))
        if not source or not table:
            return None, "缺少必填字段：源端编号、表名"
        if source != batch["源端编号"] or table != batch["表名"]:
            return None, (
                f"批次 {batch['批次编号']} 属于 {batch['源端编号']}/{batch['表名']}，"
                f"不能用于 {source}/{table} 的续读确认"
            )
        cursor = self.get_cursor(source, table)
        return {
            "批次编号": batch["批次编号"],
            "源端编号": source,
            "表名": table,
            "确认位点": cursor.get("确认位点") if cursor else None,
            "数据游标": cursor.get("数据游标") if cursor else None,
            "已读区间": batch.get("已读区间"),
            "未确认区间": batch.get("未确认区间"),
            "状态": batch["状态"],
        }, "批次与来源、表名匹配"

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _find_batch(self, batch_id: int | None) -> dict[str, Any] | None:
        for row in store.rows(BATCHES):
            if int(row.get("id", 0)) == int(batch_id or 0):
                return row
        return None

    def _active_batch(self, source: str, table: str) -> dict[str, Any] | None:
        """同一来源 + 表名只允许一个未闭合的接管批次占用续读位置。"""
        for row in store.rows(BATCHES):
            if (
                row.get("源端编号") == source
                and row.get("表名") == table
                and row.get("状态") != TERMINAL_STATUS
            ):
                return row
        return None

    def _next_batch_code(self) -> str:
        number = max((int(row.get("id", 0)) for row in store.rows(BATCHES)), default=0) + 1
        return f"TO-{number:04d}"

    def _scope_bindings(
        self, source: str, table: str, values: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]] | str:
        old_id = _text(values.get("旧绑定编号"))
        new_id = _text(values.get("新绑定编号"))
        rows = store.rows(BINDINGS)
        if not any(row.get("表名") == table for row in rows):
            return f"表 {table} 没有登记接入端绑定"

        def pick(binding_id: str, role: str) -> dict[str, Any] | str:
            if binding_id:
                for row in rows:
                    if row.get("绑定编号") == binding_id:
                        # 绑定条件必须包含来源：同名但不同来源的绑定不能顶替。
                        if row.get("源端编号") != source or row.get("表名") != table:
                            return (
                                f"绑定 {binding_id} 属于 {row.get('源端编号')}/{row.get('表名')}，"
                                f"不能用于 {source}/{table} 的接管"
                            )
                        return row
                return f"绑定 {binding_id} 不存在"
            matched = [
                row for row in rows
                if row.get("表名") == table and row.get("源端编号") == source
                and row.get("角色") == role
            ]
            if not matched:
                return f"源端 {source} 表 {table} 缺少{role}"
            matched.sort(key=lambda row: int(row.get("位点") or 0), reverse=(role == "新绑定"))
            return matched[0]

        old_binding = pick(old_id, "旧绑定")
        if isinstance(old_binding, str):
            return old_binding
        new_binding = pick(new_id, "新绑定")
        if isinstance(new_binding, str):
            return new_binding
        if old_binding["绑定编号"] == new_binding["绑定编号"]:
            return "旧绑定与新绑定不能是同一条接入端"
        return old_binding, new_binding

    def _scope_events(
        self, binding_id: str, source: str, table: str, start: int | None, end: int | None
    ) -> list[dict[str, Any]]:
        rows = [
            row
            for row in store.rows(EVENTS)
            if row.get("绑定编号") == binding_id
            and row.get("源端编号") == source
            and row.get("表名") == table
            and _in_interval(int(row.get("位点", 0)), start, end)
        ]
        return sorted(rows, key=lambda row: (int(row.get("序号", 0)), int(row.get("位点", 0))))

    @staticmethod
    def _verified_changes(
        old_events: Iterable[dict[str, Any]], new_events: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        old_by_change = {str(row["变更编号"]): row for row in old_events}
        new_by_change = {str(row["变更编号"]): row for row in new_events}
        shared = sorted(
            set(old_by_change) & set(new_by_change),
            key=lambda change: (
                int(old_by_change[change].get("序号", 0)),
                int(old_by_change[change].get("位点", 0)),
            ),
        )
        verified: list[dict[str, Any]] = []
        for change in shared:
            old_row, new_row = old_by_change[change], new_by_change[change]
            verified.append({
                "变更编号": change,
                "序号": int(old_row.get("序号", 0)),
                "旧端位点": int(old_row.get("位点", 0)),
                "新端位点": int(new_row.get("位点", 0)),
                "事件类型": new_row.get("事件类型"),
                "event_time": new_row.get("event_time"),
            })
        return verified

    @staticmethod
    def _unconfirmed_intervals(
        old_binding: dict[str, Any],
        old_events: list[dict[str, Any]],
        verified: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not old_events or old_binding.get("状态") != "已断开":
            return []
        verified_old_end = max((int(item["旧端位点"]) for item in verified), default=None)
        if verified_old_end is None:
            return []
        # 旧连接若已显式交出尾位点，且覆盖了最后一条旧端事件，则不再挂未确认区间。
        handed_tail = old_binding.get("尾位点")
        if handed_tail is not None and int(handed_tail) >= max(
            int(row["位点"]) for row in old_events
        ):
            return []
        # 旧连接在交出尾位点前断开：最后一个已核对位点之后仍归旧绑定负责。
        return [{
            "绑定编号": old_binding["绑定编号"],
            "源端编号": old_binding["源端编号"],
            "表名": old_binding["表名"],
            "起始位点": verified_old_end + 1,
            "结束位点": old_binding.get("尾位点"),
            "原因": "旧连接在交出尾位点前断开，尾段仍由旧绑定负责",
        }]

    def _close_old_tail(
        self, batch: dict[str, Any], tail: int, note: str
    ) -> tuple[dict[str, Any], str]:
        batch["旧端尾位点"] = tail
        batch["未确认区间"] = []
        batch["补齐记录"].append({"尾位点": tail, "时间": _now(), "说明": note})
        if batch.get("确认位点") is not None:
            batch["状态"] = TERMINAL_STATUS
        elif batch.get("已交付变更"):
            batch["状态"] = "续读中"
        else:
            batch["状态"] = "待续读"
        return batch, "旧端尾段已补齐，交接范围闭合"

    @staticmethod
    def _binding_snapshot(binding: dict[str, Any]) -> dict[str, Any]:
        return {
            "绑定编号": binding.get("绑定编号"),
            "源端编号": binding.get("源端编号"),
            "表名": binding.get("表名"),
            "连接地址": binding.get("连接地址"),
            "角色": binding.get("角色"),
            "位点": binding.get("位点"),
            "状态": binding.get("状态"),
        }

    def _batch_view(self, batch: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy(batch)

    def _key_mismatch(self, batch: dict[str, Any], values: dict[str, Any]) -> str | None:
        source = _text(values.get("源端编号"))
        table = _text(values.get("表名"))
        if source and source != batch["源端编号"]:
            return f"批次 {batch['批次编号']} 属于源端 {batch['源端编号']}，不能用于源端 {source}"
        if table and table != batch["表名"]:
            return f"批次 {batch['批次编号']} 属于表 {batch['表名']}，不能用于表 {table}"
        return None

    def _candidates(
        self, batch: dict[str, Any], cursor: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        delivered = {
            _delivery_key(item["源端编号"], item["表名"], item["变更编号"])
            for item in store.rows(DELIVERED)
        }
        rows = [
            row
            for row in store.rows(EVENTS)
            if row.get("绑定编号") == batch["新绑定"]["绑定编号"]
            and row.get("源端编号") == batch["源端编号"]
            and row.get("表名") == batch["表名"]
            and int(batch["续读起点"]) <= int(row.get("位点", 0)) <= int(batch["续读上限"])
        ]
        rows.sort(key=lambda row: (int(row.get("位点", 0)), int(row.get("序号", 0))))

        candidates: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            change = str(row["变更编号"])
            record = self._delivery_view(batch, row)
            key = _delivery_key(batch["源端编号"], batch["表名"], change)
            if key in delivered or change in seen:
                duplicates.append(record)
                continue
            seen.add(change)
            candidates.append(record)
        return candidates, duplicates

    def _coverage(self, batch: dict[str, Any]) -> dict[str, Any]:
        expected = [
            str(row["变更编号"])
            for row in store.rows(EVENTS)
            if row.get("绑定编号") == batch["新绑定"]["绑定编号"]
            and row.get("源端编号") == batch["源端编号"]
            and row.get("表名") == batch["表名"]
            and int(batch["续读起点"]) <= int(row.get("位点", 0)) <= int(batch["续读上限"])
        ]
        delivered = {
            item["变更编号"]
            for item in store.rows(DELIVERED)
            if item.get("源端编号") == batch["源端编号"]
            and item.get("表名") == batch["表名"]
        }
        missing = sorted({change for change in expected if change not in delivered})
        return {"完整": not missing, "缺少": missing}

    @staticmethod
    def _delivery_view(batch: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        verified = next(
            (item for item in batch["已核对变更"] if item["变更编号"] == event["变更编号"]), None
        )
        return {
            "变更编号": event["变更编号"],
            "序号": int(event.get("序号", 0)),
            "源端编号": batch["源端编号"],
            "表名": batch["表名"],
            "绑定编号": batch["新绑定"]["绑定编号"],
            "位点": int(event.get("位点", 0)),
            "旧端位点": verified["旧端位点"] if verified else None,
            "事件类型": event.get("事件类型"),
            "event_time": event.get("event_time"),
        }

    def _record_delivery(
        self,
        batch: dict[str, Any],
        cursor: dict[str, Any],
        event: dict[str, Any],
        delivered_at: str,
    ) -> bool:
        delivered = store.rows(DELIVERED)
        key = _delivery_key(event["源端编号"], event["表名"], event["变更编号"])
        if any(
            _delivery_key(row["源端编号"], row["表名"], row["变更编号"]) == key
            for row in delivered
        ):
            return False
        record = dict(event)
        record["批次编号"] = batch["批次编号"]
        record["交付时间"] = delivered_at
        delivered.append(record)

        if event["变更编号"] not in {item["变更编号"] for item in batch["已交付变更"]}:
            batch["已交付变更"].append(dict(event))
        batch["已交付位点"] = max(
            int(batch.get("已交付位点") or 0), int(event.get("位点") or 0)
        )
        return True

    def _events_for_changes(
        self, batch: dict[str, Any], changes: list[Any]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        wanted = {str(change) for change in changes}
        rows = [
            row
            for row in store.rows(EVENTS)
            if row.get("绑定编号") == batch["新绑定"]["绑定编号"]
            and row.get("源端编号") == batch["源端编号"]
            and row.get("表名") == batch["表名"]
            and str(row.get("变更编号")) in wanted
            and int(batch["续读起点"]) <= int(row.get("位点", 0)) <= int(batch["续读上限"])
        ]
        found = {str(row["变更编号"]) for row in rows}
        unknown = sorted(wanted - found)
        return [self._delivery_view(batch, row) for row in rows], unknown

    @staticmethod
    def _last_verified_sequence(batch: dict[str, Any]) -> int:
        return max((int(item["序号"]) for item in batch["已核对变更"]), default=0)

    def _cursor_view(self, source: str, table: str) -> dict[str, Any]:
        cursor = self.get_cursor(source, table) or {}
        batches = [
            row for row in store.rows(BATCHES)
            if row.get("源端编号") == source and row.get("表名") == table
        ]
        old_responsibility: list[dict[str, Any]] = []
        read_intervals: list[dict[str, Any]] = []
        for batch in batches:
            for interval in batch.get("未确认区间") or []:
                old_responsibility.append({"批次编号": batch["批次编号"], **interval})
            if batch.get("已读区间"):
                read_intervals.append({"批次编号": batch["批次编号"], **batch["已读区间"]})
        return {
            "源端编号": source,
            "表名": table,
            "确认位点": cursor.get("确认位点"),
            "数据游标": cursor.get("数据游标"),
            "已消费变更": list(cursor.get("已消费变更") or []),
            "确认批次": cursor.get("确认批次"),
            "已读区间": read_intervals,
            "仍由旧绑定负责": old_responsibility,
            "当前绑定": {
                "旧绑定": self._latest_binding(source, table, "旧绑定"),
                "新绑定": self._latest_binding(source, table, "新绑定"),
            },
        }

    def _latest_binding(self, source: str, table: str, role: str) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(BINDINGS)
            if row.get("源端编号") == source and row.get("表名") == table
            and row.get("角色") == role
        ]
        if not rows:
            return None
        rows.sort(key=lambda row: int(row.get("位点") or 0), reverse=(role == "新绑定"))
        return self._binding_snapshot(rows[0])
