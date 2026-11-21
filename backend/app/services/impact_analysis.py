"""影响分析领域服务：血缘驱动的影响分析报告、变更风险评估和补救建议生成。

接收血缘追踪的输出，对变更进行风险评估。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _issue_service():
    # 延迟导入：影响分析与问题追踪通过「派发」接口协作，而不是共享写单逻辑。
    from app.services.issue_track import IssueTrackService

    return IssueTrackService()


class ImpactAnalysisService(BaseModuleService):
    MODULE = "impact_analysis"
    ENTITY = "影响报告"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["报告编号", "变更资产", "影响下游", "影响类型", "严重级别", "影响行数", "补救建议", "生成时间"]
    REQUIRED_FIELDS = ["报告编号", "变更资产", "影响下游"]
    STATUS_ORDER = ["待生成", "已生成", "已审核", "已处理"]
    ACTION_RULES = {"触发生成": "已生成", "标记审核": "已审核", "确认处理": "已处理"}
    NEGATIVE_ACTIONS = []

    def analyze_impact(self, asset_code: str, change_type: str = "schema_change") -> dict[str, Any]:
        """分析资产变更对下游的影响（Pipeline 7 核心方法）。"""
        # 获取血缘关系中的下游资产
        downstream = []
        for row in store.rows("lineage_track"):
            if row.get("上游资产") == asset_code or row.get("下游资产") == asset_code:
                ds = row.get("下游资产") if row.get("上游资产") == asset_code else row.get("上游资产")
                if ds and ds != asset_code:
                    downstream.append(ds)

        severity = "严重" if len(downstream) >= 5 else ("需关注" if len(downstream) >= 2 else "一般")

        report_id = store.next_id(self.MODULE)
        report = {
            "id": report_id,
            "报告编号": f"IMPA-{report_id:04d}",
            "变更资产": asset_code,
            "影响下游": ", ".join(downstream) if downstream else "无",
            "影响类型": change_type,
            "严重级别": severity,
            "影响行数": str(len(downstream) * 100),
            "补救建议": self._generate_remedy(change_type, downstream),
            "生成时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已生成",
            "pending": True,
            "abnormal": severity == "严重",
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(report)

        # 如果严重级别高，把有效范围交给问题追踪派发工单
        dispatch = None
        if severity == "严重":
            dispatch = self._dispatch_impact(report, asset_code, downstream)

        store.log_change(
            self.MODULE, report_id, "影响分析",
            before=None, after=f"变更{asset_code}影响{len(downstream)}个下游, 严重级别:{severity}",
            source="pipeline_7", scope="impact_analysis"
        )

        self.emit_event("impact_analyzed", {
            "report_id": report_id,
            "asset": asset_code,
            "downstream_count": len(downstream),
            "severity": severity,
        })

        result = {
            "report": report,
            "downstream": downstream,
            "severity": severity,
            "pipeline": "lineage_track->impact_analysis",
        }
        if dispatch is not None:
            result["issue"] = dispatch["issue"]
            result["dispatch"] = dispatch
        return result

    def _generate_remedy(self, change_type: str, downstream: list[str]) -> str:
        """生成补救建议。"""
        if not downstream:
            return "无下游影响，无需特殊处理"
        count = len(downstream)
        if change_type == "schema_change":
            return f"上游Schema变更影响{count}个下游资产，建议先通知各下游负责人确认兼容性，再同步更新下游Schema"
        elif change_type in ("data_deletion", "field_removal"):
            return f"上游删除操作影响{count}个下游资产，建议先备份现有数据再执行，并通知各下游调整数据消费逻辑"
        return f"上游变更影响{count}个下游资产，建议逐项评估影响后有序推进"

    def _create_impact_issue(self, report: dict[str, Any], asset_code: str, downstream: list[str]) -> dict[str, Any]:
        """把有效范围交给问题追踪，由后者按来源报告幂等地生成派发工单。"""
        return self._dispatch_impact(report, asset_code, downstream)["issue"]

    def _dispatch_impact(
        self, report: dict[str, Any], asset_code: str, downstream: list[str]
    ) -> dict[str, Any]:
        return _issue_service().receive_impact_dispatch(
            report,
            downstream,
            source=asset_code,
            severity=str(report.get("严重级别") or "严重"),
        )

    def dispatch_report(self, report_id: int) -> dict[str, Any] | None:
        """按报告重新派发（重试入口）：复用来源对应的工单，不新建重复单。"""
        report = self.get_entry(report_id)
        if report is None:
            return None
        downstream = [d.strip() for d in str(report.get("影响下游") or "").split(",") if d.strip()]
        return (
            _issue_service().receive_impact_dispatch(
                report,
                downstream,
                source=str(report.get("变更资产") or ""),
                severity=str(report.get("严重级别") or "严重"),
            )
        )

    def confirm_dispatch(self, report_id: int, confirmed_by: str = "", note: str = "") -> dict[str, Any]:
        """按报告接收派发确认；迟到或重复的确认只落在来源报告那一侧。"""
        return _issue_service().confirm_impact_dispatch(
            report_id, confirmed_by=confirmed_by, note=note
        )

    def get_dispatch_status(self, report_id: int) -> dict[str, Any]:
        """查看报告派发与回执状态，重试时可定位尚欠确认的一侧。"""
        return _issue_service().get_impact_dispatch(report_id)

    def get_reports_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有影响报告。"""
        return store.find_by_field(self.MODULE, "变更资产", asset_code)

    def get_pending_reports(self) -> list[dict[str, Any]]:
        """获取待处理的影响报告。"""
        return [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("待生成", "已生成")
        ]