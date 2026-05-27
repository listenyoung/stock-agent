"""Simple tool permission policy."""


class ToolPermissionPolicy:
    """Classifies whether a tool can run automatically."""

    def can_auto_run(self, tool_name: str) -> bool:
        # Read-only research tools are safe to auto-run. Future write/notify/trade
        # tools should return False here and require explicit approval.
        return tool_name in {
            "get_stock_basic",
            "get_stock_daily",
            "get_news_sentiment",
            "get_market_overview",
            "get_market_latest_analysis",
            "get_financial_indicator",
            "get_recent_backtest_results",
        }

    def requires_approval(self, tool_name: str) -> bool:
        return not self.can_auto_run(tool_name)

    def approval_reason(self, tool_name: str) -> str:
        reasons = {
            "submit_backtest": "该工具会提交异步回测任务，占用计算资源，需要确认。",
        }
        return reasons.get(tool_name, "该工具可能产生副作用或较高成本，需要确认。")
