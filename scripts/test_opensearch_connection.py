"""Utility script: OpenSearch connection test and alias management."""

from __future__ import annotations

import argparse
import json
import sys

from opensearchpy import OpenSearch


DEFAULT_HOST = "159.223.47.188"
DEFAULT_PORT = 9200
DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "AVNS_1unywEqMDAepzpSW6vU"
DEFAULT_ALIAS_NAME = "welcome_all"
DEFAULT_ALIAS_INDICES = ["s_welcome_1st", "s_welcome_2nd"]


def build_client(host: str, port: int, username: str, password: str) -> OpenSearch:
    """Instantiate an OpenSearch client (SSL verification disabled for convenience)."""
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=(username, password),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )


def print_cluster_info(client: OpenSearch) -> None:
    """Fetch and print cluster information."""
    info = client.info()
    print(json.dumps(info, indent=2, ensure_ascii=False))


def create_alias(client: OpenSearch, alias: str, indices: list[str]) -> None:
    """Create or update an alias to point at the provided indices (supports wildcards)."""
    actions = [{"add": {"index": idx, "alias": alias}} for idx in indices]
    client.indices.update_aliases(body={"actions": actions})
    print(f"✅ alias '{alias}' now points to: {', '.join(indices)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenSearch helper script.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="OpenSearch port")
    parser.add_argument("--user", default=DEFAULT_USER, help="OpenSearch username")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="OpenSearch password")
    parser.add_argument(
        "--info",
        action="store_true",
        help="Print cluster info and exit (default action if nothing else is specified).",
    )
    parser.add_argument(
        "--create-alias",
        action="store_true",
        help="Create/update an alias (defaults to welcome_all)",
    )
    parser.add_argument(
        "--alias-name",
        default=DEFAULT_ALIAS_NAME,
        help=f"Alias name (default: {DEFAULT_ALIAS_NAME})",
    )
    parser.add_argument(
        "--indices",
        default=",".join(DEFAULT_ALIAS_INDICES),
        help="Comma-separated list of indices or patterns (supports wildcards)",
    )

    args = parser.parse_args()

    try:
        client = build_client(args.host, args.port, args.user, args.password)

        performed_action = False
        if args.create_alias:
            index_list = [item.strip() for item in args.indices.split(",") if item.strip()]
            create_alias(client, args.alias_name, index_list)
            performed_action = True

        if args.info or not performed_action:
            print_cluster_info(client)

        return 0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"❌ OpenSearch 작업 실패: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
