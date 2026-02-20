from __future__ import annotations

from pathlib import Path

from mailbot_v26.features import FeatureFlags
from mailbot_v26.storage.knowledge_query import KnowledgeQuery


DEFAULT_DB_PATH = Path("database.sqlite")


def _print_top_senders(query: KnowledgeQuery) -> None:
    print("Top senders:")
    senders = query.top_senders()
    if not senders:
        print("- (no data)")
        return
    for row in senders:
        print(
            f"- {row.get('from_email')} — {row.get('total_emails', 0)} писем "
            f"(🔴:{row.get('red_count', 0)} 🟡:{row.get('yellow_count', 0)} "
            f"🔵:{row.get('blue_count', 0)})"
        )


def _print_priority_distribution(query: KnowledgeQuery) -> None:
    print("Priority distribution:")
    distribution = query.priority_distribution()
    print(f"🔴 {distribution.get('🔴', 0)}")
    print(f"🟡 {distribution.get('🟡', 0)}")
    print(f"🔵 {distribution.get('🔵', 0)}")


def _print_shadow_stats(query: KnowledgeQuery) -> None:
    print("Shadow vs LLM:")
    stats = query.shadow_vs_llm_stats()
    print(f"Shadow differs: {stats.get('shadow_diff_pct', 0)}%")
    print(f"Shadow higher: {stats.get('shadow_higher_pct', 0)}%")


def _print_recent_actions(query: KnowledgeQuery) -> None:
    print("Recent actions:")
    actions = query.recent_actions()
    if not actions:
        print("- (no data)")
        return
    for row in actions:
        print(
            f"- \"{row.get('subject')}\" → {row.get('action_line')} "
            f"({row.get('priority')})"
        )


def main() -> None:
    flags = FeatureFlags()
    if not getattr(flags, "ENABLE_CRM_DIAGNOSTICS", False):
        print("CRM diagnostics disabled. Enable ENABLE_CRM_DIAGNOSTICS to run the report.")
        return

    query = KnowledgeQuery(DEFAULT_DB_PATH)

    print("=== LETTERBOT CRM DIAGNOSTICS ===\n")
    _print_top_senders(query)
    print()
    _print_priority_distribution(query)
    print()
    _print_shadow_stats(query)
    print()
    _print_recent_actions(query)


if __name__ == "__main__":
    main()
