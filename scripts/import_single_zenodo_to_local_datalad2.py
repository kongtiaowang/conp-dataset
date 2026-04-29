#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import urllib.request


def clean_title(title: str) -> str:
    return re.sub(r"\W|^(?=\d)", "_", title)


def html_to_text(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def natural_size(num_bytes: int) -> tuple[float, str]:
    units = ["Bytes", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    return (round(size, 2), unit)


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def download_file(url: str, path: str):
    with urllib.request.urlopen(url, timeout=60) as response, open(path, "wb") as out:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)


def run(cmd: list[str], cwd: str | None = None):
    subprocess.run(cmd, cwd=cwd, check=True)


def capture(cmd: list[str], cwd: str | None = None) -> str:
    return subprocess.check_output(cmd, cwd=cwd, text=True)


def ensure_dataset_dir(dataset_dir: str):
    """Ensure dataset_dir exists and is a valid DataLad dataset."""
    os.makedirs(dataset_dir, exist_ok=True)
    git_dir = os.path.join(dataset_dir, ".git")
    datalad_dir = os.path.join(dataset_dir, ".datalad")
    
    if not (os.path.isdir(git_dir) and os.path.isdir(datalad_dir)):
        parent_dir = os.path.dirname(dataset_dir)
        dataset_name = os.path.basename(dataset_dir)
        print(f"Creating DataLad dataset: {dataset_name} in {parent_dir}")
        run(["datalad", "create", dataset_name], cwd=parent_dir)


def build_description(record: dict) -> dict:
    metadata = record["metadata"]
    files = list(record.get("files", []))

    latest_version_doi = None
    version = metadata.get("relations", {}).get("version", [])
    if version:
        latest_version_doi = version[0].get("last_child", {}).get("pid_value")

    file_formats = sorted(
        {
            os.path.splitext(item.get("key", ""))[1][1:]
            for item in files
            if os.path.splitext(item.get("key", ""))[1][1:]
        }
    )

    keywords = [{"value": value} for value in metadata.get("keywords", [])]

    is_about = []
    for subject in metadata.get("subjects", []):
        identifier = subject.get("identifier", "")
        if re.match("www.ncbi.nlm.nih.gov/taxonomy", identifier):
            is_about.append(
                {
                    "identifier": {"identifier": identifier},
                    "name": subject.get("term", ""),
                }
            )
        else:
            is_about.append(
                {
                    "valueIRI": identifier,
                    "value": subject.get("term", ""),
                }
            )

    total_size = sum(item.get("size", 0) for item in files)
    dataset_size, dataset_unit = natural_size(total_size)

    creators = [{"name": item["name"]} for item in metadata.get("creators", [])]
    for contributor in metadata.get("contributors", []):
        if contributor.get("type") == "ProjectLeader":
            for creator in creators:
                if creator["name"].lower() == contributor["name"].lower():
                    creator["roles"] = [{"value": "Principal Investigator"}]
                    break
            else:
                creators.append(
                    {
                        "name": contributor["name"],
                        "roles": [{"value": "Principal Investigator"}],
                    }
                )

    identifier = record.get("conceptdoi") or record.get("doi")
    date_created = dt.datetime.strptime(record["created"], "%Y-%m-%dT%H:%M:%S.%f%z")
    date_modified = dt.datetime.strptime(record["updated"], "%Y-%m-%dT%H:%M:%S.%f%z")

    return {
        "identifier": {
            "identifier": f"https://doi.org/{identifier}",
            "identifierSource": "DOI",
        },
        "concept_doi": record["conceptrecid"],
        "latest_version": latest_version_doi,
        "title": metadata["title"],
        "files": files,
        "doi_badge": identifier,
        "creators": creators,
        "description": metadata["description"],
        "version": metadata.get("version", "None"),
        "licenses": [{"name": metadata.get("license", {}).get("id", "None")}],
        "keywords": keywords,
        "distributions": [
            {
                "formats": [
                    item.upper()
                    for item in file_formats
                    if item not in ["NIfTI", "BigWig"]
                ],
                "size": dataset_size,
                "unit": {"value": dataset_unit},
                "access": {
                    "landingPage": record.get("links", {}).get("self_html", "n/a"),
                    "authorizations": [{"value": "public"}],
                },
            }
        ],
        "extraProperties": [
            {
                "category": "logo",
                "values": [
                    {
                        "value": "https://about.zenodo.org/static/img/logos/zenodo-gradient-round.svg"
                    }
                ],
            },
            {"category": "CONP_status", "values": [{"value": "Canadian"}]},
            {"category": "subjects", "values": [{"value": "unknown"}]},
        ],
        "dates": [
            {
                "date": date_created.strftime("%Y-%m-%d %H:%M:%S"),
                "type": {"value": "date created"},
            },
            {
                "date": date_modified.strftime("%Y-%m-%d %H:%M:%S"),
                "type": {"value": "date modified"},
            },
        ],
        "isAbout": is_about,
    }


def create_tracker(path: str, dataset: dict):
    with open(path, "w") as f:
        json.dump(
            {
                "zenodo": {
                    "concept_doi": dataset.get("concept_doi"),
                    "version": dataset.get("latest_version"),
                },
                "title": dataset.get("title"),
            },
            f,
            indent=4,
        )


def write_readme(path: str, dataset: dict):
    content = """# {0}

[![DOI](https://www.zenodo.org/badge/DOI/{1}.svg)](https://doi.org/{1})

Crawled from Zenodo

## Description

{2}
""".format(
        dataset["title"],
        dataset["doi_badge"],
        html_to_text(dataset["description"]).replace("\n", "<br />"),
    )
    with open(path, "w") as f:
        f.write(content)


def write_dats(path: str, dataset: dict, dataset_dir: str):
    dats_fields = {
        "identifier",
        "title",
        "description",
        "creators",
        "types",
        "version",
        "licenses",
        "keywords",
        "distributions",
        "extraProperties",
        "dates",
        "isAbout",
    }
    data = {key: value for key, value in dataset.items() if key in dats_fields}
    annex_list = capture(["git", "annex", "list"], cwd=dataset_dir).splitlines()
    file_paths = [line.split(" ")[-1] for line in annex_list if " " in line]
    num_files = len(file_paths)
    data.setdefault("extraProperties", []).append(
        {"category": "files", "values": [{"value": str(num_files)}]}
    )
    data.setdefault("types", [{"information": {"value": "unknown"}}])

    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--record", required=True)
    parser.add_argument("--dataset-dir", required=True)
    args = parser.parse_args()

    dataset_dir = os.path.abspath(args.dataset_dir)
    ensure_dataset_dir(dataset_dir)

    record = fetch_json(f"https://zenodo.org/api/records/{args.record}")
    dataset = build_description(record)

    for item in dataset["files"]:
        target_name = item["key"]
        target_path = os.path.join(dataset_dir, target_name)
        if os.path.exists(target_path):
            continue
        download_file(item["links"]["self"], target_path)
        if target_name in {"Readme.md", "DATS.json"}:
            run(["git", "add", target_name], cwd=dataset_dir)
        else:
            run(["git", "annex", "add", target_name], cwd=dataset_dir)

    create_tracker(os.path.join(dataset_dir, ".conp-zenodo-crawler.json"), dataset)
    write_readme(os.path.join(dataset_dir, "Readme.md"), dataset)
    write_dats(os.path.join(dataset_dir, "DATS.json"), dataset, dataset_dir)

    run(["git", "add", ".conp-zenodo-crawler.json", "Readme.md", "DATS.json"], cwd=dataset_dir)
    run(["git", "commit", "-m", f"Import Zenodo record {args.record}"], cwd=dataset_dir)

    print(clean_title(dataset["title"]))
    print(dataset["title"])


if __name__ == "__main__":
    main()
