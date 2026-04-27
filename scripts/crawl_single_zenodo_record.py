#!/usr/bin/env python3
import argparse
import json
import os
import sys

import requests


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the CONP Zenodo crawler for exactly one Zenodo record."
    )
    parser.add_argument("--record", required=True, help="Zenodo record id, e.g. 19440997")
    parser.add_argument(
        "--repo",
        required=True,
        help="Path to the local conp-dataset clone/fork",
    )
    parser.add_argument(
        "--config",
        default=os.path.expanduser("~/.conp_crawler_config.json"),
        help="Path to crawler config JSON",
    )
    parser.add_argument(
        "--github-token",
        default=os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN"),
        help="GitHub token with repo permissions",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--no-pr", action="store_true")
    return parser.parse_args()


def ensure_config(path: str, repo: str, github_token: str | None):
    data = {}
    if os.path.isfile(path):
        with open(path) as f:
            data = json.load(f)

    data["conp-dataset_path"] = repo
    if github_token:
        data["github_token"] = github_token

    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def main():
    args = parse_args()

    if not args.github_token:
        raise SystemExit(
            "Missing GitHub token. Pass --github-token or set GITHUB_TOKEN/GH_TOKEN."
        )

    repo = os.path.abspath(args.repo)
    if not os.path.isdir(repo):
        raise SystemExit(f"Repository path does not exist: {repo}")

    os.environ["BASEDIR"] = repo
    ensure_config(args.config, repo, args.github_token)

    sys.path.insert(0, repo)

    from scripts.Crawlers.ZenodoCrawler import ZenodoCrawler  # noqa: E402

    class SingleRecordZenodoCrawler(ZenodoCrawler):
        def _query_zenodo(self):
            url = f"https://zenodo.org/api/records/{args.record}"
            if self.verbose:
                print(f"Zenodo query: {url}")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return [response.json()]

    crawler = SingleRecordZenodoCrawler(
        args.github_token,
        args.config,
        args.verbose,
        args.force,
        args.no_pr,
        repo,
    )
    crawler.run()


if __name__ == "__main__":
    main()
