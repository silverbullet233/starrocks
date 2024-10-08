---
displayed_sidebar: docs
sidebar_position: 50
---

# 更新表

建表时需要定义唯一键。当多条数据具有相同的唯一键时，value 列会进行 REPLACE，查询时返回唯一键相同的一组数据中的最新数据。并且支持单独定义排序键，如果查询的过滤条件包含排序键，则 StarRocks 能够快速地过滤数据，提高查询效率。

更新表能够支撑实时和频繁更新的场景，不过目前已经逐渐被[主键表](./primary_key_table.md)代替。

## 适用场景

实时和频繁更新的业务场景，例如分析电商订单。在电商场景中，订单的状态经常会发生变化，每天的订单更新量可突破上亿。

## 原理

更新表可以视为聚合表的特殊情况，value 列指定的聚合函数为 REPLACE，返回具有相同唯一键的一组数据中的最新数据。

数据分批次多次导入至更新表，每一批次数据分配一个版本号，因此同一唯一键的数据可能有多个版本，查询时返回版本最新（即版本号最大）的数据。

例如下表中，`ID` 是唯一键，`value` 是指标列，`_version` 是 StarRocks 内部的版本号。其中，`ID` 为 1 的数据有两个导入批次，版本号分别为 `1` 和 `2`；`ID` 为 `2` 的数据有三个导入批次，版本号分别为 `3`、`4`、`5`。

| ID   | value | _version |
| ---- | ----- | -------- |
| 1    | 100   | 1        |
| 1    | 101   | 2        |
| 2    | 100   | 3        |
| 2    | 101   | 4        |
| 2    | 102   | 5        |

查询 `ID` 为 `1` 的数据时，仅会返回最新版本 `2` 的数据，而查询 `ID` 为 `2` 的数据时，仅会返回最新版本 `5` 的数据，最终查询结果如下：

| ID   | value |
| ---- | ----- |
| 1    | 101   |
| 2    | 102   |

## 创建表

在电商订单分析场景中，经常按照日期对订单状态进行统计分析，则可以将经常使用的过滤字段订单创建时间 `create_time`、订单编号 `order_id` 作为唯一键，其余列订单状态 `order_state` 和订单总价 `total_price` 作为指标列。这样既能够满足实时更新订单状态的需求，又能够在查询中进行快速过滤。

在该业务场景下，建表语句如下：

```SQL
CREATE TABLE orders (
    create_time DATE NOT NULL COMMENT "create time of an order",
    order_id BIGINT NOT NULL COMMENT "id of an order",
    order_state INT COMMENT "state of an order",
    total_price BIGINT COMMENT "price of an order"
)
UNIQUE KEY(create_time, order_id)
DISTRIBUTED BY HASH(order_id); 
```

> **注意**
>
> - 建表时必须使用 `DISTRIBUTED BY HASH` 子句指定分桶键。分桶键的更多说明，请参见[分桶](../data_distribution/Data_distribution.md#分桶)。
> - 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [设置分桶数量](../data_distribution/Data_distribution.md#设置分桶数量)。

## 使用说明

- **唯一键**：
  - 在建表语句中，唯一键必须定义在其他列之前。
  - 唯一键需要通过 `UNIQUE KEY` 显式定义。
  - 唯一键必须满足唯一性约束。
- **排序键**：

  - 自 v3.3.0 起，更新表解耦了排序键和聚合键。更新表支持使用 `ORDER BY` 指定排序键和使用 `UNIQUE KEY` 指定唯一键。排序键和唯一键中的列需要保持一致，但是列的顺序不需要保持一致。

  - 查询时，在聚合之前数据就能基于排序键进行过滤，而通常在多版本聚合之后数据才能基于 value 列进行过滤，因此建议将频繁使用的过滤字段作为排序键，在聚合前就能过滤数据，从而提升查询性能。
- 建表时，仅支持为 key 列创建 Bitmap 索引、Bloom filter 索引。

## 下一步

建表完成后，您可以创建多种导入作业，导入数据至表中。具体导入方式，请参见[导入方案](../../loading/loading_introduction/Loading_intro.md)。

:::note

- 导入数据时，仅支持全部更新，即导入任务需要指明所有列，例如示例中的 `create_time`、`order_id`、`order_state` 和 `total_price` 四个列。
- 在设计导入频率时，建议以满足业务对实时性的要求为准。查询更新表的数据时，需要聚合多版本的数据，当版本过多时会导致查询性能降低。所以导入数据至更新表时，应该适当降低导入频率，从而提升查询性能。如果业务对实时性的要求是分钟级别，那么每分钟导入一次更新数据即可，不需要秒级导入。

:::
