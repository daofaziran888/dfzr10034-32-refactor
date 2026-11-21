"""问题追踪领域服务：数据治理问题的发现、指派、修复和闭环验证管理。

接收质量规则检查阻断、影响分析和合规扫描等过程的自动化问题创建。
"""
from __future__ import annotations

import threading
import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class IssueTrackService(BaseModuleService):
    MODULE = "issue_track"
    ENTITY = "治理问题"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["问题编号", "关联资产", "问题类型", "严重级别", "指派处理", "当前状态", "修复方案", "截止日期"]
    REQUIRED_FIELDS = ["问题编号", "关联资产", "问题类型"]
    STATUS_ORDER = ["待确认", "处理中", "已修复", "已验证"]
    ACTION_RULES = {"确认问题": "处理中", "指派处理": "处理中", "验证修复": "已验证"}
    NEGATIVE_ACTIONS = []

    IMPACT_ISSUE_TYPE = "影响范围过大"
    # 同一进程内并发重送同一份影响报告时，保证找单与建单是一个整体。
    _dispatch_lock = threading.Lock()

    def create_issue(self, asset_code: str, issue_type: str, severity: str,
                     assignee: str, solution: str, deadline_days: int = 7) -> dict[str, Any]:
        """创建治理问题。"""
        issue_id = store.next_id(self.MODULE)
        issue = {
            "id": issue_id,
            "问题编号": f"ISSU-{issue_id:04d}",
            "关联资产": asset_code,
            "问题类型": issue_type,
            "严重级别": severity,
            "指派处理": assignee,
            "当前状态": "待确认",
            "修复方案": solution,
            "截止日期": _time.strftime("%Y-%m-%d", _time.localtime(_time.time() + 86400 * deadline_days)),
            "status": "待确认",
            "pending": True,
            "abnormal": severity == "严重",
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(issue)

        # 发出事件通知
        self.emit_event("issue_created", {
            "issue_id": issue_id,
            "asset_code": asset_code,
            "issue_type": issue_type,
            "severity": severity,
            "assignee": assignee,
        })

        return issue

    # ── 影响报告派发 / 回执 ────────────────────────────────────
    #
    # 影响分析只负责圈定有效范围，工单由这里统一生成。幂等键是「来源报告」，
    # 不是下游资产：同一份报告并发重送只落一张单，不同报告即使指向同一下游
    # 也各自成单。派发结论回写到影响报告（整改待办），回执按报告反查。

    def receive_impact_dispatch(
        self,
        report: dict[str, Any],
        downstream: list[str] | None = None,
        *,
        source: str | None = None,
        severity: str | None = None,
    ) -> dict[str, Any]:
        """接收影响分析派发的有效范围，按来源报告幂等生成/复用影响问题工单。

        下游资产只是影响范围，不参与去重：I04 与 I05 即便都指向月底销售宽表，
        也各自对应一张问题单；重复送达 I04 时复用 I04 的那一张。
        """
        effective_scope = self._normalize_scope(
            downstream if downstream is not None else report.get("影响下游")
        )
        source_topic = source or str(report.get("变更资产") or "")
        report_id = report.get("id")
        report_no = str(report.get("报告编号") or "")

        with self._dispatch_lock:
            existing = self._find_impact_issue(report)
            if existing is not None:
                issue = existing
                created = False
            else:
                issue = self._append_impact_issue(
                    report=report,
                    source_topic=source_topic,
                    downstream=effective_scope,
                    severity=severity or str(report.get("严重级别") or "严重"),
                )
                created = True

            self._write_dispatch_back(report, issue, created=created)

            self.emit_event("impact_issue_dispatched", {
                "issue_id": issue["id"],
                "report_id": report_id,
                "report_no": report_no,
                "source": source_topic,
                "created": created,
                "downstream": effective_scope,
            })

            return {
                "ok": True,
                "created": created,
                "reused": not created,
                "issue_id": issue["id"],
                "issue": issue,
                "report_id": report_id,
                "report_no": report_no,
                "source": issue.get("来源主题", source_topic),
                "downstream": effective_scope,
                "dispatch_status": "已新建" if created else "已复用",
                "confirmed": issue.get("确认状态") == "已确认",
                "pending_confirmation": issue.get("确认状态") != "已确认",
                "pipeline": "impact_analysis->issue_track",
            }

    def confirm_impact_dispatch(
        self,
        report: int | str | dict[str, Any],
        *,
        confirmed_by: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        """按来源报告接收派发确认（回执）。

        确认消息迟到时也只回填发出该报告的那张工单，不会误确认同一下游的
        独立报告。重复确认保持幂等。
        """
        with self._dispatch_lock:
            issues = sorted(self._find_impact_issues(report), key=lambda row: int(row.get("id", 0)))
            if not issues:
                return {
                    "ok": False,
                    "found": False,
                    "confirmed": False,
                    "message": "未找到该影响报告对应的派发工单",
                    "report": self._report_key(report),
                }

            pending = [row for row in issues if row.get("确认状态") != "已确认"]
            target = pending[0] if pending else issues[0]
            already_confirmed = not pending or target.get("确认状态") == "已确认"

            if not already_confirmed:
                target["确认状态"] = "已确认"
                target["回执状态"] = "已确认"
                target["确认人"] = confirmed_by
                target["回执时间"] = _time.strftime("%Y-%m-%d %H:%M:%S")
                if note:
                    target["回执说明"] = note

            self._write_receipt_back(target, confirmed=not already_confirmed)

            remaining = [row["id"] for row in issues if row.get("确认状态") != "已确认"]

            return {
                "ok": True,
                "found": True,
                "already_confirmed": already_confirmed,
                "issue_id": target["id"],
                "issue": target,
                "report_id": target.get("impact_report_id"),
                "report_no": target.get("来源报告", ""),
                "source": target.get("来源主题", ""),
                "confirmed": True,
                "pending_confirmation": bool(remaining),
                "pending_issue_ids": remaining,
            }

    def get_impact_dispatch(self, report: int | str | dict[str, Any]) -> dict[str, Any]:
        """按来源报告反查派发与回执状态，用于重试时定位尚欠确认的一侧。"""
        issues = sorted(self._find_impact_issues(report), key=lambda row: int(row.get("id", 0)))
        pending = [row for row in issues if row.get("确认状态") != "已确认"]
        current = (pending or issues or [None])[0]
        return {
            "report": self._report_key(report),
            "dispatched": bool(issues),
            "issue_id": current["id"] if current else None,
            "issue_ids": [row["id"] for row in issues],
            "issue": current,
            "confirmed": bool(issues) and not pending,
            "pending_confirmation": bool(pending),
            "pending_issue_id": pending[0]["id"] if pending else None,
            "source": current.get("来源主题", "") if current else "",
        }

    def get_impact_issues_by_report(self, report: int | str | dict[str, Any]) -> list[dict[str, Any]]:
        """按来源报告反查影响问题明细。"""
        return sorted(self._find_impact_issues(report), key=lambda row: int(row.get("id", 0)))

    # 语义别名，方便按「回执 / 派单」两种叫法检索。
    def dispatch_receipt(self, report: int | str | dict[str, Any]) -> dict[str, Any]:
        return self.get_impact_dispatch(report)

    def get_dispatch_status(self, report: int | str | dict[str, Any]) -> dict[str, Any]:
        return self.get_impact_dispatch(report)

    def get_issues_by_impact_report(self, report: int | str | dict[str, Any]) -> list[dict[str, Any]]:
        return self.get_impact_issues_by_report(report)

    def dispatch_impact_report(
        self,
        report: dict[str, Any],
        downstream: list[str] | None = None,
        *,
        source: str | None = None,
        severity: str | None = None,
    ) -> dict[str, Any]:
        """`receive_impact_dispatch` 的别名，语义上更贴近「把报告派发出去」。"""
        return self.receive_impact_dispatch(
            report, downstream, source=source, severity=severity)

    # ── 内部：影响问题工单 ────────────────────────────────────

    def _normalize_scope(self, downstream: Any) -> list[str]:
        """把影响分析交付的有效范围规整为资产列表。"""
        if downstream is None:
            return []
        if isinstance(downstream, str):
            text = downstream.strip()
            if not text or text == "无":
                return []
            return [part.strip() for part in text.split(",") if part.strip()]
        return [str(item) for item in downstream if str(item).strip()]

    def _report_key(self, report: int | str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(report, dict):
            return {"id": report.get("id"), "报告编号": report.get("报告编号")}
        if isinstance(report, int):
            return {"id": report, "报告编号": ""}
        text = str(report)
        # 纯数字只当报告 id，避免把编号误当成数字 id 去匹配。
        if text.isdigit():
            return {"id": int(text), "报告编号": ""}
        return {"id": None, "报告编号": text}

    def _impact_issue_rows(self) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(self.MODULE)
            if row.get("问题类型") == self.IMPACT_ISSUE_TYPE
        ]

    def _issue_matches_report(self, issue: dict[str, Any], key: dict[str, Any]) -> bool:
        report_id = key.get("id")
        report_no = str(key.get("报告编号") or "")
        if report_id is not None:
            issue_report_id = issue.get("impact_report_id")
            if issue_report_id is not None:
                try:
                    if int(issue_report_id) == int(report_id):
                        return True
                except (TypeError, ValueError):
                    pass
        if report_no and str(issue.get("来源报告") or "") == report_no:
            return True
        return False

    def _find_impact_issues(self, report: int | str | dict[str, Any]) -> list[dict[str, Any]]:
        key = self._report_key(report)
        if key.get("id") is None and not key.get("报告编号"):
            return []
        return [
            row for row in self._impact_issue_rows()
            if self._issue_matches_report(row, key)
        ]

    def _find_impact_issue(self, report: dict[str, Any]) -> dict[str, Any] | None:
        issues = self._find_impact_issues(report)
        return issues[0] if issues else None

    def _append_impact_issue(
        self,
        *,
        report: dict[str, Any],
        source_topic: str,
        downstream: list[str],
        severity: str,
    ) -> dict[str, Any]:
        issue_id = store.next_id(self.MODULE)
        asset_code = str(report.get("变更资产") or source_topic)
        issue = {
            "id": issue_id,
            "问题编号": f"ISSU-IMP-{issue_id:04d}",
            "关联资产": asset_code,
            "问题类型": self.IMPACT_ISSUE_TYPE,
            "严重级别": severity,
            "指派处理": "数据架构师",
            "当前状态": "待评估",
            "修复方案": f"上游{asset_code}变更影响{len(downstream)}个下游，需逐项评估",
            "截止日期": _time.strftime("%Y-%m-%d", _time.localtime(_time.time() + 86400 * 5)),
            "status": "待确认",
            "pending": True,
            "abnormal": True,
            # 来源标识：去重与回执都按报告而不是按下游。
            "impact_report_id": report.get("id"),
            "来源报告": str(report.get("报告编号") or ""),
            "来源主题": source_topic,
            "来源下游": ", ".join(downstream) if downstream else "无",
            "派单状态": "已派发",
            "派单时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "确认状态": "待确认",
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(issue)
        return issue

    def _write_dispatch_back(
        self, report: dict[str, Any], issue: dict[str, Any], *, created: bool
    ) -> None:
        """把派发结论回写到整改待办（影响报告）。

        首次派发的结论保留在「首次派发结论」，重试只更新「派发结论」与时间，
        这样回看时既能找到原有派单，也能看出这次是重试还是新建。
        """
        now = _time.strftime("%Y-%m-%d %H:%M:%S")
        conclusion = "新建工单" if created else "复用已有工单"
        report["issue_id"] = issue["id"]
        report["问题编号"] = issue.get("问题编号", "")
        report["派发状态"] = "已派发"
        report["派发结论"] = conclusion
        report.setdefault("首次派发结论", conclusion)
        report["最后派发时间"] = now
        report.setdefault("派发时间", now)
        if issue.get("确认状态") != "已确认":
            report["确认状态"] = "待确认"

        # 存储层里可能存在同一报告的另一份字典实例，一并回写。
        for row in store.rows("impact_analysis"):
            if row is report:
                continue
            if report.get("id") is not None and row.get("id") == report.get("id"):
                row.setdefault("issue_id", issue["id"])
                row.setdefault("问题编号", issue.get("问题编号", ""))
                row.setdefault("首次派发结论", conclusion)
                row["派发状态"] = "已派发"
                row["派发结论"] = conclusion
                row["最后派发时间"] = now
                row.setdefault("派发时间", now)

    def _write_receipt_back(self, issue: dict[str, Any], *, confirmed: bool) -> None:
        """确认回执回写到整改待办（影响报告）。"""
        if not confirmed:
            return
        now = _time.strftime("%Y-%m-%d %H:%M:%S")
        report_id = issue.get("impact_report_id")
        source_report = str(issue.get("来源报告") or "")
        for row in store.rows("impact_analysis"):
            same_id = report_id is not None and row.get("id") == report_id
            same_no = bool(source_report) and str(row.get("报告编号") or "") == source_report
            if same_id or same_no:
                row["确认状态"] = "已确认"
                row["回执状态"] = "已确认"
                row["回执时间"] = now
                row.setdefault("issue_id", issue["id"])

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """沿用通用动作流转；影响问题被确认时把回执回写到来源报告。"""
        entry, message = super().run_action(entry_id, action)
        if entry is not None and entry.get("问题类型") == self.IMPACT_ISSUE_TYPE:
            # 「确认问题」即下游确认收到该报告派发的工单，按来源报告回写回执。
            if action == "确认问题" and entry.get("确认状态") != "已确认":
                self.confirm_impact_dispatch(
                    entry.get("impact_report_id") or entry.get("来源报告", ""),
                    confirmed_by=str(entry.get("指派处理") or ""),
                )
        return entry, message

    def fix_issue(self, issue_id: int, fix_description: str) -> dict[str, Any] | None:
        """标记问题为已修复。"""
        issue = self.get_entry(issue_id)
        if not issue:
            return None

        old_status = issue.get("status", "")
        issue["status"] = "已修复"
        issue["pending"] = False
        issue["修复方案"] = f"{issue.get('修复方案', '')} | 实际修复: {fix_description}"
        issue["fixed_at"] = _time.strftime("%Y-%m-%d %H:%M:%S")

        store.log_change(
            self.MODULE, issue_id, "修复问题",
            before=old_status, after="已修复",
            source="fix", scope="issue_track"
        )

        self.emit_event("issue_fixed", {
            "issue_id": issue_id,
            "asset_code": issue.get("关联资产"),
        })

        return issue

    def verify_issue(self, issue_id: int, verified_by: str) -> dict[str, Any] | None:
        """验证修复完成。"""
        issue = self.get_entry(issue_id)
        if not issue:
            return None

        issue["status"] = "已验证"
        issue["pending"] = False
        issue["verified_by"] = verified_by
        issue["verified_at"] = _time.strftime("%Y-%m-%d %H:%M:%S")

        self.emit_event("issue_verified", {
            "issue_id": issue_id,
            "asset_code": issue.get("关联资产"),
        })

        return issue

    def get_issues_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有治理问题。"""
        return store.find_by_field(self.MODULE, "关联资产", asset_code)

    def get_open_issues(self) -> list[dict[str, Any]]:
        """获取所有未关闭问题。"""
        return [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("待确认", "处理中")
        ]

    def get_issues_by_severity(self, severity: str) -> list[dict[str, Any]]:
        """按严重级别获取问题。"""
        return store.find_by_field(self.MODULE, "严重级别", severity)

    def get_issues_by_assignee(self, assignee: str) -> list[dict[str, Any]]:
        """按指派人获取问题。"""
        return store.find_by_field(self.MODULE, "指派处理", assignee)

    def issue_statistics(self) -> dict[str, Any]:
        """问题统计概览。"""
        all_issues = store.rows(self.MODULE)
        by_status = store.count_by_status(self.MODULE)
        by_severity = store.count_by_field(self.MODULE, "严重级别")

        return {
            "total": len(all_issues),
            "open": sum(1 for r in all_issues if r.get("status") in ("待确认", "处理中")),
            "resolved": sum(1 for r in all_issues if r.get("status") in ("已修复", "已验证")),
            "by_status": by_status,
            "by_severity": by_severity,
            "pipeline": "quality_rule->issue_track",
        }