"""源端接管校验的验收数据集。

这里放两张同名但不同来源的表（`orders`），以及换绑期间新旧接入端
各自读到的重叠变更。数据只用于演示与验收：

* ``SOUR-MYSQL-01/orders``：新旧两端都核对到前两笔变更，第三笔只在
  新端出现；旧连接在交出尾位点之前断开。
* ``SOUR-KAFKA-02/orders``：另一来源的同名表，用于验证确认与游标
  不能跨来源借用。

所有私有表都以 ``_`` 开头，`Store.module_names` 不会把它们当成业务模块。
"""
from __future__ import annotations

from typing import Any

TAKEOVER_SEED_ROWS: dict[str, list[dict[str, Any]]] = {
    # 接入端绑定：绑定条件里必须带来源，不能只凭表名匹配。
    "_takeover_bindings": [
        {
            "绑定编号": "BIND-OLD-01",
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "连接地址": "mysql://old-db:3306/app",
            "角色": "旧绑定",
            "位点": 200,
            "状态": "已断开",
        },
        {
            "绑定编号": "BIND-NEW-01",
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "连接地址": "mysql://new-db:3306/app",
            "角色": "新绑定",
            "位点": 310,
            "状态": "在线",
        },
        {
            "绑定编号": "BIND-K-OLD",
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "连接地址": "kafka://old-broker:9092/orders",
            "角色": "旧绑定",
            "位点": 920,
            "状态": "已断开",
        },
        {
            "绑定编号": "BIND-K-NEW",
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "连接地址": "kafka://new-broker:9092/orders",
            "角色": "新绑定",
            "位点": 930,
            "状态": "在线",
        },
    ],
    # 两端事件：同一笔业务变更在旧/新接入端可能有不同位点，按变更编号对齐。
    "_takeover_events": [
        {
            "event_id": 1,
            "变更编号": "ORD-CHG-1",
            "序号": 1,
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "绑定编号": "BIND-OLD-01",
            "位点": 100,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:00:01Z",
        },
        {
            "event_id": 2,
            "变更编号": "ORD-CHG-1",
            "序号": 1,
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "绑定编号": "BIND-NEW-01",
            "位点": 110,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:00:01Z",
        },
        {
            "event_id": 3,
            "变更编号": "ORD-CHG-2",
            "序号": 2,
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "绑定编号": "BIND-OLD-01",
            "位点": 200,
            "事件类型": "UPDATE",
            "event_time": "2026-11-16T08:00:02Z",
        },
        {
            "event_id": 4,
            "变更编号": "ORD-CHG-2",
            "序号": 2,
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "绑定编号": "BIND-NEW-01",
            "位点": 210,
            "事件类型": "UPDATE",
            "event_time": "2026-11-16T08:00:02Z",
        },
        {
            "event_id": 5,
            "变更编号": "ORD-CHG-3",
            "序号": 3,
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "绑定编号": "BIND-NEW-01",
            "位点": 310,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:00:05Z",
        },
        # 同名但不同来源的表：游标和确认必须与 SOUR-MYSQL-01 完全隔离。
        {
            "event_id": 6,
            "变更编号": "KAF-CHG-1",
            "序号": 1,
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "绑定编号": "BIND-K-OLD",
            "位点": 900,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:01:00Z",
        },
        {
            "event_id": 7,
            "变更编号": "KAF-CHG-1",
            "序号": 1,
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "绑定编号": "BIND-K-NEW",
            "位点": 910,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:01:00Z",
        },
        {
            "event_id": 8,
            "变更编号": "KAF-CHG-2",
            "序号": 2,
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "绑定编号": "BIND-K-OLD",
            "位点": 920,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:01:02Z",
        },
        {
            "event_id": 9,
            "变更编号": "KAF-CHG-2",
            "序号": 2,
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "绑定编号": "BIND-K-NEW",
            "位点": 930,
            "事件类型": "INSERT",
            "event_time": "2026-11-16T08:01:02Z",
        },
    ],
    # CDC 续读位置：按（源端编号 + 表名）隔离，同名表不会互相推进。
    "_cdc_cursors": [
        {
            "id": 1,
            "源端编号": "SOUR-MYSQL-01",
            "表名": "orders",
            "确认位点": 0,
            "数据游标": 0,
            "绑定": None,
            "确认批次": None,
            "已消费变更": [],
        },
        {
            "id": 2,
            "源端编号": "SOUR-KAFKA-02",
            "表名": "orders",
            "确认位点": 0,
            "数据游标": 0,
            "绑定": None,
            "确认批次": None,
            "已消费变更": [],
        },
    ],
    # CDC 实际交付过的业务变更，用于换绑重试时去重。
    "_cdc_delivered": [],
    # 接管位点批次清单。
    "_takeover_batches": [],
}
