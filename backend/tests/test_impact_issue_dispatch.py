"""影响报告派发 / 回执关联测试。

覆盖的约束：
- 影响分析只交付有效范围，工单由问题追踪按「来源报告」幂等生成；
- 同一份报告并发重送只留一张来源对应的工单；
- 独立报告即使指向同一下游也不得合并；
- 派发结论回写整改待办，迟到/重复的确认只落在来源报告那一侧；
- 重试可定位尚欠确认的一侧。
"""
from concurrent.futures import ThreadPoolExecutor

import app.store as store_mod
from app.services.impact_analysis import ImpactAnalysisService
from app.services.issue_track import IssueTrackService


IMPACT_TYPE = IssueTrackService.IMPACT_ISSUE_TYPE
DOWNSTREAM = "月底销售宽表"


def _fresh():
    store_mod.store.reset()
    return ImpactAnalysisService(), IssueTrackService()


def _add_report(report_id: int, report_no: str, source_topic: str, downstream: str = DOWNSTREAM) -> dict:
    report = {
        "id": report_id,
        "报告编号": report_no,
        "变更资产": source_topic,
        "影响下游": downstream,
        "影响类型": "schema_change",
        "严重级别": "严重",
        "status": "已生成",
        "pending": True,
        "abnormal": True,
    }
    store_mod.store.rows("impact_analysis").append(report)
    return report


def _impact_issues() -> list[dict]:
    return [r for r in store_mod.store.rows("issue_track") if r.get("问题类型") == IMPACT_TYPE]


class TestImpactIssueDispatch:

    def test_analysis_hands_scope_to_issue_track(self):
        impact, _ = _fresh()
        for index in range(5):
            store_mod.store.rows("lineage_track").append({
                "id": 100 + index, "上游资产": "SRC", "下游资产": f"D{index}", "影响层级": "1",
            })
        result = impact.analyze_impact("SRC", "schema_change")

        assert result["severity"] == "严重"
        issue = result["issue"]
        assert issue["问题类型"] == IMPACT_TYPE
        assert issue["来源报告"] == result["report"]["报告编号"]
        assert issue["来源主题"] == "SRC"
        assert issue["来源下游"] == ", ".join(result["downstream"])
        # 派发结论回写整改待办
        assert result["report"]["派发状态"] == "已派发"
        assert result["report"]["issue_id"] == issue["id"]
        assert result["report"]["首次派发结论"] == "新建工单"

    def test_non_severe_report_creates_no_dispatch(self):
        impact, _ = _fresh()
        store_mod.store.rows("lineage_track").append({
            "id": 1, "上游资产": "ONE", "下游资产": "X", "影响层级": "1",
        })
        result = impact.analyze_impact("ONE")
        assert result["severity"] == "一般"
        assert "issue" not in result
        assert _impact_issues() == []

    def test_same_report_resent_keeps_single_issue(self):
        _, issues = _fresh()
        report = _add_report(404, "IMPA-I04", "主题域A")

        first = issues.receive_impact_dispatch(report, [DOWNSTREAM], source="主题域A")
        retry = issues.receive_impact_dispatch(report, [DOWNSTREAM], source="主题域A")

        assert first["created"] is True
        assert retry["created"] is False and retry["reused"] is True
        assert retry["issue_id"] == first["issue_id"]
        assert len(_impact_issues()) == 1
        # 重试仍能看出原有派单
        assert report["首次派发结论"] == "新建工单"
        assert report["派发结论"] == "复用已有工单"

    def test_concurrent_resend_leaves_one_issue(self):
        _, issues = _fresh()
        _add_report(404, "IMPA-I04", "主题域A")

        def send(_):
            return issues.receive_impact_dispatch(
                store_mod.store.find("impact_analysis", 404), [DOWNSTREAM], source="主题域A")

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(send, range(24)))

        assert len({r["issue_id"] for r in results}) == 1
        assert sum(1 for r in results if r["created"]) == 1
        assert len(_impact_issues()) == 1

    def test_independent_reports_on_same_downstream_stay_separate(self):
        _, issues = _fresh()
        report4 = _add_report(404, "IMPA-I04", "主题域A")
        report5 = _add_report(505, "IMPA-I05", "主题域B")

        dispatch4 = issues.receive_impact_dispatch(report4, [DOWNSTREAM], source="主题域A")
        dispatch5 = issues.receive_impact_dispatch(report5, [DOWNSTREAM], source="主题域B")

        assert dispatch4["issue_id"] != dispatch5["issue_id"]
        assert len(_impact_issues()) == 2
        # 同一下游不足以判定为一次派发
        assert dispatch4["downstream"] == dispatch5["downstream"] == [DOWNSTREAM]

    def test_confirm_only_lands_on_source_report(self):
        _, issues = _fresh()
        report4 = _add_report(404, "IMPA-I04", "主题域A")
        report5 = _add_report(505, "IMPA-I05", "主题域B")
        dispatch4 = issues.receive_impact_dispatch(report4, [DOWNSTREAM], source="主题域A")
        dispatch5 = issues.receive_impact_dispatch(report5, [DOWNSTREAM], source="主题域B")

        receipt = issues.confirm_impact_dispatch("IMPA-I04", confirmed_by="下游负责人")

        assert receipt["issue_id"] == dispatch4["issue_id"]
        assert report4["确认状态"] == "已确认"
        assert report5["确认状态"] == "待确认"

        # I05 反查仍是独立来源，且尚欠确认
        status5 = issues.get_impact_dispatch("IMPA-I05")
        assert status5["issue_id"] == dispatch5["issue_id"]
        assert status5["source"] == "主题域B"
        assert status5["pending_confirmation"] is True

        # 迟到 / 重复的确认幂等
        again = issues.confirm_impact_dispatch("IMPA-I04")
        assert again["already_confirmed"] is True
        assert again["confirmed"] is True
        assert len(_impact_issues()) == 2

    def test_issue_detail_exposes_source_and_original_dispatch(self):
        impact, issues = _fresh()
        report = _add_report(404, "IMPA-I04", "主题域A")
        first = issues.receive_impact_dispatch(report, [DOWNSTREAM], source="主题域A")
        retry = impact.dispatch_report(404)

        detail = issues.get_impact_issues_by_report(404)[0]
        assert detail["来源报告"] == "IMPA-I04"
        assert detail["来源主题"] == "主题域A"
        assert detail["来源下游"] == DOWNSTREAM
        assert detail["派单状态"] == "已派发"

        assert first["dispatch_status"] == "已新建"
        assert retry["dispatch_status"] == "已复用"
        assert retry["issue_id"] == first["issue_id"]

    def test_report_retry_locates_unconfirmed_side(self):
        impact, issues = _fresh()
        _add_report(404, "IMPA-I04", "主题域A")
        _add_report(505, "IMPA-I05", "主题域B")
        impact.dispatch_report(404)
        impact.dispatch_report(505)

        impact.confirm_dispatch(404, confirmed_by="下游负责人")

        assert impact.get_dispatch_status(404)["confirmed"] is True
        pending = impact.get_dispatch_status(505)
        assert pending["confirmed"] is False
        assert pending["pending_confirmation"] is True
        assert pending["pending_issue_id"] is not None

    def test_issue_action_confirmation_writes_receipt_back(self):
        _, issues = _fresh()
        report = _add_report(404, "IMPA-I04", "主题域A")
        dispatch = issues.receive_impact_dispatch(report, [DOWNSTREAM], source="主题域A")

        entry, _ = issues.run_action(dispatch["issue_id"], "确认问题")

        assert entry["status"] == "处理中"
        assert report["确认状态"] == "已确认"
        assert report["回执状态"] == "已确认"
        assert issues.get_impact_dispatch(404)["confirmed"] is True

    def test_redispatch_after_confirmation_keeps_receipt(self):
        _, issues = _fresh()
        report = _add_report(404, "IMPA-I04", "主题域A")
        issues.receive_impact_dispatch(report, [DOWNSTREAM], source="主题域A")
        issues.confirm_impact_dispatch(404)

        retry = issues.receive_impact_dispatch(report, [DOWNSTREAM], source="主题域A")

        assert retry["reused"] is True
        assert retry["confirmed"] is True
        assert report["确认状态"] == "已确认"
        assert report["派发结论"] == "复用已有工单"

    def test_new_analysis_is_a_new_report_and_new_issue(self):
        impact, issues = _fresh()
        for index in range(5):
            store_mod.store.rows("lineage_track").append({
                "id": 100 + index, "上游资产": "SRC", "下游资产": f"D{index}", "影响层级": "1",
            })
        first = impact.analyze_impact("SRC")
        second = impact.analyze_impact("SRC")  # 一次新的结构变化

        assert first["report"]["id"] != second["report"]["id"]
        assert first["issue"]["id"] != second["issue"]["id"]
        assert len(_impact_issues()) == 2
