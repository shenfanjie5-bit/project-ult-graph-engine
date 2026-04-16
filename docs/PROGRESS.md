# graph-engine 项目进度总览

> **最后更新**: 2026-04-17
> **项目文档**: [graph-engine.project-doc.md](./graph-engine.project-doc.md)
> **任务拆解**: [TASK_BREAKDOWN.md](./TASK_BREAKDOWN.md)

## 里程碑进度

| 阶段 | 里程碑 | 目标 | Issue 数量 | 状态 | 退出条件 |
|------|--------|------|-----------|------|----------|
| 阶段 0 | milestone-0 | 图谱性能与模型 Spike | 2 | **未开始** | Lite 规模传播 < 1 分钟 |
| 阶段 1 | milestone-1 | P3a 图谱主干 | 2 | 未开始 | 候选变更进入正式图并完成快照回写 |
| 阶段 2 | milestone-2 | P3a 运维闭环 | 2 | 未开始 | Neo4j 可清空/重建/校验/ready |
| 阶段 3 | milestone-3 | P3b 完整传播 | 2 | 未开始 | 正式 impact snapshot 稳定产出 |
| 阶段 4 | milestone-4 | P3b 只读服务 | 2 | 未开始 | 事件驱动链路可读图谱但不写正式图 |

## Issue 状态明细

| Issue | 标题 | 里程碑 | 优先级 | 状态 | 依赖 |
|-------|------|--------|--------|------|------|
| ISSUE-001 | 项目基础设施、Neo4j 连接层、Schema 定义与核心领域模型 | milestone-0 | P0 | **未开始** | 无 |
| ISSUE-002 | GDS 规模压测脚本与传播预算验证 | milestone-0 | P0 | 未开始 | #001 |
| ISSUE-003 | Graph Promotion — CandidateGraphDelta 到正式图的提升与 Live Graph 同步 | milestone-1 | P0 | 未开始 | #001 |
| ISSUE-004 | 单通道传播引擎与 Snapshot 生成回写 | milestone-1 | P0 | 未开始 | #003 |
| ISSUE-005 | Neo4j Graph Status 状态机与一致性校验 | milestone-2 | P0 | 未开始 | #001 |
| ISSUE-006 | Cold Reload 全量重建与 GDS Projection 恢复 | milestone-2 | P0 | 未开始 | #005 |
| ISSUE-007 | Event 与 Reflexive 传播通道实现 | milestone-3 | P1 | 未开始 | #004 |
| ISSUE-008 | 三通道融合、Regime-aware 权重调整与完整 Impact Snapshot | milestone-3 | P1 | 未开始 | #007 |
| ISSUE-009 | 子图查询与传播路径查询接口 | milestone-4 | P1 | 未开始 | #005 |
| ISSUE-010 | 只读局部传播模拟与消费方接入样例 | milestone-4 | P1 | 未开始 | #008, #009 |

## 依赖关系图

```
ISSUE-001 (基础设施 + 模型)
  ├── ISSUE-002 (GDS 压测)
  ├── ISSUE-003 (Graph Promotion)
  │     └── ISSUE-004 (单通道传播 + Snapshot)
  │           └── ISSUE-007 (Event + Reflexive 通道)
  │                 └── ISSUE-008 (三通道融合)
  │                       └── ISSUE-010 (只读模拟)
  └── ISSUE-005 (Status 状态机)
        ├── ISSUE-006 (Cold Reload)
        └── ISSUE-009 (子图查询)
              └── ISSUE-010 (只读模拟)
```

## 关键性能预算 (§19.1)

| 指标 | 目标值 | 当前状态 |
|------|--------|----------|
| Phase 1 单轮传播耗时 | < 60 秒 | 待验证 (ISSUE-002) |
| Phase 0 一致性校验耗时 | < 5 秒 | 待验证 (ISSUE-002) |
| Cold Reload 典型耗时 | < 90 秒 | 待验证 (ISSUE-002) |
| Cold Reload 硬超时 | < 5 分钟 | 待验证 (ISSUE-002) |

## 质量指标 (§19.2)

| 指标 | 目标值 | 当前状态 |
|------|--------|----------|
| ready 状态下 Neo4j / Iceberg 计数偏差率 | 0 | 未实现 |
| 事件驱动局部模拟误写正式图次数 | 0 | 未实现 |
| graph_snapshot 产出成功率 | 100% | 未实现 |
| 反向代码依赖事件数 | 0 | 符合 |

## 验收标准检查表 (§23)

- [ ] 1. 冻结后的 CandidateGraphDelta 先写入 Layer A，再同步到 Neo4j live graph
- [ ] 2. 稳定生成并回写 graph_snapshot 与 graph_impact_snapshot
- [ ] 3. neo4j_graph_status、consistency check、Cold Reload 闭环可运行
- [ ] 4. 三类传播都有正式实现，并能使用只读 regime context
- [ ] 5. graph-engine 不反向 import main-core
- [ ] 6. Full 模式局部图模拟保持只读
- [ ] 7. 模块边界与主项目 12 + N 模块边界一致
