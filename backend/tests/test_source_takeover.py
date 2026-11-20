"""源端接管校验：换绑交接、重叠区间、同名跨来源隔离与重启核验。"""
from __future__ import annotations

import pytest

from app.services.source_takeover import SourceTakeoverService
from app.store import store

MYSQL = "SOUR-MYSQL-01"
KAFKA = "SOUR-KAFKA-02"


@pytest.fixture()
def service() -> SourceTakeoverService:
    return SourceTakeoverService()


def prepare(service, **overrides):
    payload = {"源端编号": MYSQL, "表名": "orders", "采集区间": {"开始位点": 0, "结束位点": 10000}}
    payload.update(overrides)
    batch, message = service.prepare(payload)
    assert batch is not None, message
    return batch


def test_prepare_covers_overlapping_interval(service):
    batch = prepare(service)
    assert batch["源端编号"] == MYSQL
    assert [item["变更编号"] for item in batch["已核对变更"]] == ["ORD-CHG-1", "ORD-CHG-2"]
    assert batch["旧绑定"]["绑定编号"] == "BIND-OLD-01"
    assert batch["新绑定"]["绑定编号"] == "BIND-NEW-01"
    assert batch["旧端区间"] == {"起始位点": 100, "结束位点": 200}
    assert batch["新端区间"] == {"起始位点": 110, "结束位点": 310}
    # 整表接管时点是新端尾位点，已核对区间终点是第二笔变更。
    assert batch["整表接管时点"] == 310
    assert batch["已核对区间"]["结束位点"] == 210
    # 续读从已核对区间起点开始，第三笔不会被尾位点跳过。
    assert batch["续读起点"] == 110
    assert batch["续读上限"] == 310
    assert batch["状态"] == "待旧端补齐"
    assert batch["未确认区间"][0]["绑定编号"] == "BIND-OLD-01"


def test_prepare_requires_source_scoped_binding(service):
    batch, message = service.prepare({
        "源端编号": KAFKA, "表名": "orders", "旧绑定编号": "BIND-OLD-01",
    })
    assert batch is None
    assert "BIND-OLD-01" in message and KAFKA in message

    batch, message = service.prepare({"源端编号": MYSQL, "表名": "missing_table"})
    assert batch is None
    assert "没有登记接入端绑定" in message


def test_prepare_rejects_non_overlapping_changes(service):
    batch, message = service.prepare({
        "源端编号": MYSQL,
        "表名": "orders",
        "采集区间": {"开始位点": 220, "结束位点": 300},
    })
    assert batch is None
    assert "没有两端都核对到的变更" in message


def test_resume_delivers_third_change_from_new_binding(service):
    batch = prepare(service)
    payload, message = service.resume(batch["id"])
    assert payload is not None, message
    delivered = [event["变更编号"] for event in payload["事件"]]
    assert delivered == ["ORD-CHG-1", "ORD-CHG-2", "ORD-CHG-3"]
    assert {event["绑定编号"] for event in payload["事件"]} == {"BIND-NEW-01"}
    assert payload["跳过重复"] == []


def test_retry_does_not_deliver_business_change_twice(service):
    batch = prepare(service)
    service.resume(batch["id"])
    payload, _ = service.resume(batch["id"])
    assert payload["事件"] == []
    assert {event["变更编号"] for event in payload["跳过重复"]} == {
        "ORD-CHG-1", "ORD-CHG-2", "ORD-CHG-3",
    }
    delivered = [row["变更编号"] for row in store.rows("_cdc_delivered")]
    assert sorted(delivered) == ["ORD-CHG-1", "ORD-CHG-2", "ORD-CHG-3"]


def test_confirm_advances_only_one_cursor(service):
    batch = prepare(service)
    service.resume(batch["id"])
    confirmed, message = service.confirm(batch["id"])
    assert confirmed is not None, message
    assert confirmed["确认位点"] == 310
    # 旧端未补齐，不能标成已交接。
    assert confirmed["状态"] == "待旧端补齐"

    mysql = service.get_cursor(MYSQL, "orders")
    kafka = service.get_cursor(KAFKA, "orders")
    assert mysql["确认位点"] == 310
    assert mysql["数据游标"] == 310
    assert kafka["确认位点"] == 0 and kafka["数据游标"] == 0

    # 重试确认幂等，不再推进位点。
    again, message = service.confirm(batch["id"])
    assert again["确认位点"] == 310
    assert "重复确认未推进位点" in message


def test_confirm_rejects_expected_position_mismatch(service):
    batch = prepare(service)
    service.resume(batch["id"])
    # 模拟并发：另一个确认先把这张表的续读位置推到 310。
    cursor = service.get_cursor(MYSQL, "orders")
    cursor["确认位点"] = 310
    cursor["确认批次"] = "TO-9999"
    blocked, message = service.confirm(batch["id"], {"期望位点": 0})
    assert blocked is None
    assert "不能并发推进" in message
    assert service.get_cursor(MYSQL, "orders")["确认位点"] == 310


def test_confirm_cannot_borrow_other_tables_batch(service):
    mysql_batch = prepare(service)
    service.resume(mysql_batch["id"])
    service.confirm(mysql_batch["id"])

    kafka_batch = prepare(service, 源端编号=KAFKA)
    service.resume(kafka_batch["id"])

    blocked, message = service.confirm(kafka_batch["id"], {"源端编号": MYSQL, "表名": "orders"})
    assert blocked is None
    assert "属于源端" in message

    verified, message = service.verify({
        "批次编号": mysql_batch["id"], "源端编号": KAFKA, "表名": "orders",
    })
    assert verified is None
    assert "不能用于" in message
    assert service.get_cursor(KAFKA, "orders")["确认位点"] == 0


def test_confirm_requires_complete_delivery(service):
    batch = prepare(service)
    # 未续读、只回报前两笔已消费：区间不完整，不能推进确认位点。
    blocked, message = service.confirm(batch["id"], {"已消费变更": ["ORD-CHG-1", "ORD-CHG-2"]})
    assert blocked is None
    assert "尚未完整交付" in message
    assert service.get_cursor(MYSQL, "orders")["确认位点"] == 0


def test_confirm_rejects_change_from_other_source(service):
    batch = prepare(service)
    service.resume(batch["id"])
    blocked, message = service.confirm(batch["id"], {"已消费变更": ["KAF-CHG-1"]})
    assert blocked is None
    assert "不能计入确认" in message


def test_backfill_requires_third_change_delivered(service):
    batch = prepare(service)

    # CDC 还没消费第三笔变更时，旧端尾段不能闭合。
    pending, message = service.backfill_old_tail(batch["id"], {"尾位点": 300})
    assert pending is not None
    assert "未补齐" in message
    assert pending["未确认区间"]
    assert pending["状态"] == "待旧端补齐"

    # CDC 通过新绑定续读并确认后，尾段视为补齐，交接范围闭合。
    service.resume(batch["id"])
    service.confirm(batch["id"])
    closed, message = service.backfill_old_tail(batch["id"], {"尾位点": 300})
    assert "闭合" in message
    assert closed["状态"] == "已交接"
    assert closed["未确认区间"] == []


def test_restart_reports_consumed_and_old_responsibility(service):
    batch = prepare(service)
    service.resume(batch["id"])
    service.confirm(batch["id"])

    state = service.restart()
    mysql = next(row for row in state["各表"] if row["源端编号"] == MYSQL)
    kafka = next(row for row in state["各表"] if row["源端编号"] == KAFKA)

    assert mysql["已消费变更"] == ["ORD-CHG-1", "ORD-CHG-2", "ORD-CHG-3"]
    assert mysql["数据游标"] == 310
    assert mysql["仍由旧绑定负责"][0]["绑定编号"] == "BIND-OLD-01"
    assert mysql["已读区间"][0]["结束位点"] == 310

    # 同名表的另一个来源没有被推进。
    assert kafka["确认位点"] == 0
    assert kafka["数据游标"] == 0
    assert kafka["已消费变更"] == []
    assert state["一致性"]["正常"] is True, state["一致性"]["问题"]


def test_restart_does_not_widen_coverage_after_retry(service):
    batch = prepare(service)
    service.resume(batch["id"])
    service.confirm(batch["id"])
    first = service.get_cursor(MYSQL, "orders")

    service.restart()
    service.resume(batch["id"])
    service.confirm(batch["id"])
    second = service.get_cursor(MYSQL, "orders")

    assert first["确认位点"] == second["确认位点"] == 310
    assert first["数据游标"] == second["数据游标"] == 310
    assert sorted(first["已消费变更"]) == sorted(second["已消费变更"])
    assert [row["变更编号"] for row in store.rows("_cdc_delivered")] == [
        "ORD-CHG-1", "ORD-CHG-2", "ORD-CHG-3",
    ]


def test_same_table_two_sources_advance_independently(service):
    mysql_batch = prepare(service)
    kafka_batch = prepare(service, 源端编号=KAFKA)

    service.resume(mysql_batch["id"])
    service.resume(kafka_batch["id"])
    service.confirm(mysql_batch["id"])

    assert service.get_cursor(MYSQL, "orders")["确认位点"] == 310
    assert service.get_cursor(KAFKA, "orders")["确认位点"] == 0

    service.confirm(kafka_batch["id"])
    assert service.get_cursor(KAFKA, "orders")["确认位点"] == 930
    assert service.get_cursor(MYSQL, "orders")["确认位点"] == 310


def test_pipeline_resume_writes_back_and_stops_on_retry(service):
    from app.services.pipeline_orchestrator import (
        run_takeover_resume_pipeline, list_pipeline_results,
    )

    prepare(service)
    prepare(service, 源端编号=KAFKA)

    first = run_takeover_resume_pipeline()
    assert first["resumed_batches"] == 2
    assert first["delivered_events"] == 5  # MYSQL 端 3 笔，KAFKA 端 2 笔

    second = run_takeover_resume_pipeline()
    assert second["resumed_batches"] == 2
    assert second["delivered_events"] == 0
    assert all(item["交付变更"] == [] for item in second["batches"])

    delivered = [(row["源端编号"], row["变更编号"]) for row in store.rows("_cdc_delivered")]
    assert len(delivered) == len(set(delivered)) == 5
    assert any(pr["pipeline_id"] == "source_takeover_cdc" for pr in list_pipeline_results())


def test_concurrent_confirm_only_one_advances(service):
    from concurrent.futures import ThreadPoolExecutor

    batch = prepare(service)
    service.resume(batch["id"])
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: service.confirm(batch["id"]), range(4)))
    ok = [result for result in results if result[0] is not None]
    assert ok, [message for _, message in results]
    assert all(result[0]["确认位点"] == 310 for result in ok)
    assert service.get_cursor(MYSQL, "orders")["确认位点"] == 310
    assert service.get_cursor(KAFKA, "orders")["确认位点"] == 0


def test_prepare_reuses_active_batch(service):
    first = prepare(service)
    second, message = service.prepare({
        "源端编号": MYSQL, "表名": "orders",
        "采集区间": {"开始位点": 0, "结束位点": 10000},
    })
    assert second["批次编号"] == first["批次编号"]
    assert "已复用" in message
    assert len(service.list_batches()) == 1
