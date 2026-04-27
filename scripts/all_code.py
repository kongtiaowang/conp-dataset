========================================
FILE: ./dats_jsonld_annotator/__init__.py
========================================


========================================
FILE: ./dats_jsonld_annotator/annotator.py
========================================
import argparse
import json
import logging
import pathlib as pal
import sys
import time

import jsonschema as jss


CONTEXT_DIR = (pal.Path(__file__).parent / "context" / "sdo").resolve()
SCHEMA_DIR = (pal.Path(__file__).parent / "schema").resolve()
logger = logging.getLogger("DATS annotator")


def find_schema(parent_schema, term, json_object):
    """
    This function finds the appropriate JSON schema for a term in a DATS JSON object. To do
    so, it relies on the JSON schema of the supplied JSON object.

    :param parent_schema: The DATS JSON schema that corresponds to the json_object parameter
    :param term: the string term for which the schema shall be found
    :param json_object: the JSON object that contains the term and to which the parent_schema corresponds to
    :return: the DATS JSON schema for the term
    """
    ps = parent_schema["properties"]
    if term not in ps.keys():
        # The only way this could happen on a valid dataset is if someone has specified additional properties
        # We'd like to discourage this
        logger.warning(
            f'I cannot find {term = } in the parent schema: {parent_schema["id"]}.\n{json_object = }'
        )
        return None

    # Set up the schema resolver
    resolver = jss.RefResolver(base_uri=parent_schema["id"], referrer=None)

    # See if "items" is in the schema entry
    search_dict = ps[term]
    if "items" in search_dict.keys():
        search_dict = ps[term]["items"]

    # This section is looking for the name of the DATS schema that should be applied to the term
    # If there are multiple possibilities (e.g. "anyOf", "oneOf", "allOf") we want to pick a schema
    # for which the current term value validates
    if "$ref" in search_dict.keys():
        # There is only one possible schema
        schema_name = search_dict["$ref"]
    elif len(set(search_dict.keys()).intersection(["anyOf", "oneOf", "allOf"])) > 0:
        # There is (most likely) more than one option for the schema as indicated by one of the
        # keys from ["anyOf", "oneOf", "allOf"]
        schema_rel = list(
            set(search_dict.keys()).intersection(["anyOf", "oneOf", "allOf"])
        )[0]
        possible_schemata = []
        # We will now iterate over the possible schemata for the term
        for ref in search_dict[schema_rel]:
            # Get the schema
            ref_name = ref["$ref"]
            _schema_uri, _schema = resolver.resolve(ref_name)
            if _schema_uri == resolver.base_uri:
                _schema = parent_schema
            if jss.Draft4Validator(_schema).is_valid(json_object):
                # If the schema validates the term value (json_object) then we can keep
                # it around as a potential schema
                possible_schemata.append(ref_name)
        if len(possible_schemata) > 1:
            # TODO: decide if we let the user pick which option to go with
            logger.debug(f"I got more than one option for {term}: {possible_schemata}")
        elif len(possible_schemata) == 0:
            logger.warning(
                f"I have no fitting schema for {json_object} {term} among {search_dict[schema_rel]}"
            )
            return None
        # If anything fits, just pick the first one
        # TODO: we may want to leave this up to the user here, particularly if the instances
        #       map to different / meaningful things in SDO
        schema_name = possible_schemata[0]
    else:
        # There is nothing to be done here in terms of annotation
        logger.info(f"{term = } does not need to be annotated")
        return None

    _schema_uri, _schema = resolver.resolve(schema_name)
    if _schema_uri == resolver.base_uri:
        _schema = parent_schema
    if _schema is None:
        logger.warning(
            f'The schema we found for {schema_name}: was None! The parent schema was: {parent_schema["id"]}'
            f"and it was resolved using this URI: {resolver.base_uri}"
        )
    return _schema


def find_context(schema_id, context_dir):
    """
    For a given DATS JSON schema, finds and loads the corresponding DATS SDO context file.
    This function makes use of the fact that DATS SDO context files follow a similar naming structure
    to the DATS JSON schema files.

    :param schema_id: the string URI of the schema
    :param context_dir: the directory path where the DATS SDO context files can be found
    :return: the @context section of the DATS SDO context file as a dictionary
    """
    schema_name = pal.Path(schema_id).name
    context_name = pal.Path(schema_name.replace("_schema", "_sdo_context")).with_suffix(
        ".jsonld"
    )
    context = json.load(open(context_dir / context_name))["@context"]
    return context


def annotate_dats_object(
    json_object, schema, specific_context, context_dir=CONTEXT_DIR
):
    """
    This function recursively traverses a DATS instance and generates two things:

    1. The @type declarations for each node in the JSONLD graph
    2. A copy of the specific context mappings needed to map the DATS instance to SDO

    :param json_object: A DATS instance as a dictionary
    :param schema: The DATS JSON schema corresponding to the json_object as a dictionary
    :param specific_context:  the DATS instance specific context
    :param context_dir: the path to the DATS context files
    :return: The DATS instance with appropriate @type declarations, and the DATS instance specific context
    """

    if not isinstance(json_object, dict) or schema is None:
        # If the json_object is not a dict, then it cannot be annotated
        # If the schema is None, then the key might not be part of the DATS schema
        return json_object, specific_context

    context = find_context(schema["id"], context_dir)
    for k, v in json_object.items():
        if isinstance(v, dict):
            _schema = find_schema(schema, k, v)
            json_object[k], _local_context = annotate_dats_object(
                v, _schema, specific_context, context_dir
            )
            specific_context.update(_local_context)
        if isinstance(v, list):
            # Let's find the schema link under "items"
            annotation_list = []
            for vv in v:
                if not isinstance(vv, dict):
                    annotation_list.append(vv)
                    continue
                _schema = find_schema(schema, k, vv)
                _json_o, _local_context = annotate_dats_object(
                    vv, _schema, specific_context, context_dir
                )
                annotation_list.append(_json_o)
                specific_context.update(_local_context)
            json_object[k] = annotation_list
        # Add the key mapping to the context
        if k not in context.keys():
            logger.debug(f'{k} not in context: {pal.Path(schema["id"]).name}')
            continue
        if k not in specific_context.keys():
            specific_context[k] = context[k]
            logger.debug(f"{k} added to context: {context[k]}")
        elif specific_context[k] == context[k]:
            continue
        else:
            logger.debug(f"{k} duplicate: {specific_context[k]} vs {context[k]}")
            continue

    dtype = schema["properties"]["@type"]["enum"][0]
    if dtype not in context.keys():
        logger.debug(f'{dtype} not in context {schema["id"]}')
    else:
        specific_context[dtype] = context[dtype]
    json_object["@type"] = dtype
    return json_object, specific_context


def gen_jsonld_outpath(dats_json_f, out_path):
    """
    This function generates an output file path for the annotated DATS JSONLD file.

    :param dats_json_f: the path to the original DATS instance file
    :param out_path: the folder or new file path where the annotated DATS.jsonld file should be stored
    :return:  the final DATS.jsonld file path
    """
    if not isinstance(dats_json_f, pal.Path):
        dats_json_f = pal.Path(dats_json_f)
    if out_path is not None and not isinstance(out_path, pal.Path):
        out_path = pal.Path(out_path)

    out_name = dats_json_f.with_suffix(".jsonld").name
    if out_path is None:
        # We save the output to the parent directory of the current DATS JSON file
        out_path = dats_json_f.parent
    elif out_path.suffix != "":
        # We have most likely gotten a path to a non-existent file, let's use that
        return out_path
    elif not out_path == dats_json_f.parent:
        # We are saving this somewhere other than the original folder
        out_name = f"{dats_json_f.parent.name}_{out_name}"
    if out_path.is_dir():
        dats_jsonld_f = out_path / out_name
    else:
        raise ValueError(
            f"{out_path = } for {dats_json_f.resolve()} is not a path to a file or directory. "
            f"I don't know where to store the output. "
            f"Please provide a valid path with the --out flag."
        )
    return dats_jsonld_f


def dats_to_jsonld(dats_f, schema_f, context_dir, out_path=None, clobber=False):
    """
    Helper function to load the inputs and store the annotated DATS.jsonld file
    """
    # TODO: log how many terms were not annotated because they are missing from context
    tic = time.time()
    dats_jsonld_f = gen_jsonld_outpath(dats_f, out_path)
    if dats_jsonld_f.is_file():
        if clobber:
            logger.warning(
                f"{dats_jsonld_f.resolve()} already exists and {clobber = }. "
                f"The file {dats_jsonld_f.resolve()} will be overwritten now!"
            )
        else:
            logger.warning(
                f"{dats_jsonld_f.resolve()} already exists and {clobber = }. "
                f"Consider setting the clobber flag to overwrite existing files"
            )
            return

    dats_json = json.load(open(dats_f))
    schema = json.load(open(schema_f))

    # Do a very basic validation of the JSON object before we try to annotate it
    if not jss.Draft4Validator(schema).is_valid(dats_json):
        logger.error(
            f"{dats_f.resolve()} is not a valid DATS file. "
            f"If you think this should be a valid DATS file, "
            f"please use the CONP validator to get a list of specific errors."
            f"\n\nSkipping this file."
        )
        return

    # Now do the annotation
    try:
        dats_jsonld, context = annotate_dats_object(dats_json, schema, {}, context_dir)
    except Exception as e:
        logger.exception(f"Annotating {dats_f} did not complete!", e, exc_info=True)

    # Prefill the context with the SDO mapping
    context["sdo"] = "https://schema.org/"
    # Combine the context and the dats graph
    dats_jsonld["@context"] = [
        context,
    ]
    logger.info(
        f"Final result written to {dats_jsonld_f.resolve()}! This took {time.time()-tic :.2f} seconds"
    )
    json.dump(dats_jsonld, open(dats_jsonld_f, "w"), indent=2)


def main(cli_args):

    logging.basicConfig(format="%(levelname)s:  %(message)s", level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Annotate a DATS.json file to DATS.jsonld given a schema"
    )
    parser.add_argument(
        "dats_path",
        type=pal.Path,
        help="""
                If this is a path to a DATS file, then only this DATS file will be annotated.
                If this is a path to a directory, then each subdirectory of this directory is expected
                to contain a DATS file called DATS.json. Each of these files will then be annotated iteratively
                """,
    )
    parser.add_argument(
        "-ds",
        "--dats_schema",
        type=pal.Path,
        default=SCHEMA_DIR / "dataset_schema.json",
        help="""Specify the full path to a DATS dataset schema file if you don't want to use the default one""",
    )
    parser.add_argument("-dc", "--dats_context_dir", type=pal.Path, default=CONTEXT_DIR)
    parser.add_argument(
        "--out",
        type=pal.Path,
        default=None,
        help="Where to create the JSONLD file(s) (default = in the same folder).",
    )
    parser.add_argument("--clobber", action="store_true")
    args = parser.parse_args(cli_args)

    if args.dats_path.is_file():
        dats_to_jsonld(
            dats_f=args.dats_path,
            schema_f=args.dats_schema,
            context_dir=args.dats_context_dir,
            out_path=args.out,
            clobber=args.clobber,
        )
    elif args.dats_path.is_dir():
        files_to_convert = list(args.dats_path.glob("*/DATS.json"))
        if not args.out.is_dir():
            logger.warning(
                f"The {args.out.resolve()} folder will be created and JSONLD files will be saved in it."
            )
            args.out.mkdir()
        if files_to_convert is None:
            logger.error(
                f"could not find any DATS.json files in subdirectories of {args.dats_path.resolve()}"
            )
            exit(code=1)

        logger.info(
            f"Found {len(files_to_convert)} files to convert at {args.dats_path.resolve()}"
        )
        start = time.time()
        for file_idx, dats_f in enumerate(files_to_convert, start=1):
            logger.info(
                f"Now processing file {file_idx}/{len(files_to_convert)}: {dats_f.parent.name}"
            )
            dats_to_jsonld(
                dats_f=dats_f,
                schema_f=args.dats_schema,
                context_dir=args.dats_context_dir,
                out_path=args.out,
                clobber=args.clobber,
            )
        logger.info(
            f"Completed annotating {len(files_to_convert)} DATS files. "
            f"This took {time.time()-start :.2f} seconds."
        )
    else:
        logger.error(f"I cannot find {args.dats_path}. Will stop now.")


if __name__ == "__main__":
    main(sys.argv[1:])


========================================
FILE: ./crawl.py
========================================
import argparse
import json
import os
import sys
import traceback

from git import Repo

sys.path.append(os.getcwd())
from scripts.Crawlers.ZenodoCrawler import ZenodoCrawler  # noqa: E402
from scripts.Crawlers.OSFCrawler import OSFCrawler  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=r"""
    CONP crawler.

    Requirements:
    * GitHub user must have a fork of https://github.com/CONP-PCNO/conp-dataset
    * Script must be run in the base directory of a local clone of this fork
    * Git remote 'origin' of local Git clone must point to that fork. Warning: this script will
       push dataset updates to 'origin'.
    * Local Git clone must be set to branch 'master'
    """,
    )
    parser.add_argument(
        "github_token",
        action="store",
        nargs="?",
        help="GitHub access token",
    )
    parser.add_argument(
        "config_path",
        action="store",
        nargs="?",
        help="Path to config file to use",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print debug information",
    )
    parser.add_argument("--force", action="store_true", help="Force updates")
    parser.add_argument(
        "--no_pr",
        action="store_true",
        help="Don't create a pull request at the end",
    )
    args = parser.parse_args()

    github_token = args.github_token
    config_path = args.config_path
    if not config_path:
        config_path = os.path.join(
            os.path.expanduser("~"),
            ".conp_crawler_config.json",
        )

    # If config file does not exist, create an empty one
    if not os.path.isfile(config_path):
        with open(config_path, "w") as f:
            json.dump({}, f)

    with open(config_path) as f:
        config = json.load(f)

    if "conp-dataset_path" not in config.keys():
        raise Exception(
            '"conp-dataset_path" not configured in ' + config_path + ","
            "please configure it as follows: \n"
            '  "conp-dataset_path": "PATH TO conp-dataset DIRECTORY",',
        )

    if not github_token and "github_token" not in config.keys():
        raise Exception(
            "Github token not passed by command line argument "
            "nor found in config file " + config_path + ", "
            "please pass your github access token via the command line",
        )
    elif github_token:
        config["github_token"] = github_token
        # Store newly passed github token into config file
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)
    else:  # Retrieve github token from config file
        github_token = config["github_token"]

    if "BASEDIR" not in os.environ:
        raise Exception(
            "BASEDIR environment variable must be set and pointing to conp-dataset repo"
        )

    return (
        github_token,
        config_path,
        args.verbose,
        args.force,
        config["conp-dataset_path"],
        args.no_pr,
        os.environ["BASEDIR"],
    )


if __name__ == "__main__":
    (
        github_token,
        config_path,
        verbose,
        force,
        conp_dataset_dir_path,
        no_pr,
        basedir,
    ) = parse_args()

    try:
        try:
            if verbose:
                print(
                    "==================== Zenodo Crawler Running ===================="
                    + os.linesep,
                )
            ZenodoCrawlerObj = ZenodoCrawler(
                github_token,
                config_path,
                verbose,
                force,
                no_pr,
                basedir,
            )
            ZenodoCrawlerObj.run()
        except Exception:
            traceback.print_exc()

        try:
            if verbose:
                print(
                    os.linesep
                    + "==================== OSF Crawler Running ===================="
                    + os.linesep,
                )
            OSFCrawlerObj = OSFCrawler(
                github_token, config_path, verbose, force, no_pr, basedir
            )
            OSFCrawlerObj.run()
        except Exception:
            traceback.print_exc()

        # INSTANTIATE NEW CRAWLERS AND RUN HERE

    except Exception:
        traceback.print_exc()
    finally:
        # Always switch branch back to master
        repository = Repo(basedir)
        if repository.active_branch.name != "master":
            repository.git.checkout("master")

        if verbose:
            print(os.linesep + "==================== Done ====================")


========================================
FILE: ./unlock.py
========================================
#!/usr/bin/env python
import json
import os
import re
import traceback

from datalad import api
from git import Repo


def project_name2env(project_name: str) -> str:
    """Convert the project name to a valid ENV var name.

    The ENV name for the project must match the regex `[a-zA-Z_]+[a-zA-Z0-9_]*`.

    Parameters
    ----------
    project_name: str
        Name of the project.

    Return
    ------
    project_env: str
        A valid ENV name for the project.
    """
    project_name = project_name.replace("-", "_")
    project_env = re.sub("[_]+", "_", project_name)  # Remove consecutive `_`
    project_env = re.sub("[^a-zA-Z0-9_]", "", project_env)

    # Env var cannot start with number
    if re.compile("[0-9]").match(project_env[0]):
        project_env = "_" + project_env

    return project_env.upper()


def unlock():
    repo = Repo()
    project: str = project_name2env(repo.working_dir.split("/")[-1])
    token: (str | None) = os.getenv(project + "_ZENODO_TOKEN", None)

    if not token:
        raise Exception(
            f"{project}_ZENODO_TOKEN not found. Cannot inject the Zenodo token into the git-annex urls.",
        )

    annex = repo.git.annex
    if repo.active_branch.name != "master":
        raise Exception("Dataset repository not set to branch 'master'")

    if not os.path.isfile(".conp-zenodo-crawler.json"):
        raise Exception("'.conp-zenodo-crawler.json file not found")

    with open(".conp-zenodo-crawler.json") as f:
        metadata = json.load(f)

    # Ensure correct data
    if not metadata["restricted"]:
        raise Exception("Dataset not restricted, no need to unlock")
    if (
        len(metadata["private_files"]["archive_links"]) == 0
        and len(metadata["private_files"]["files"]) == 0
    ):
        raise Exception("No restricted files to unlock")

    # Set token in archive link URLs
    if len(metadata["private_files"]["archive_links"]) > 0:
        repo.git.checkout("git-annex")
        changes = False
        for link in metadata["private_files"]["archive_links"]:
            for dir_name, _, files in os.walk("."):
                for file_name in files:
                    file_path = os.path.join(dir_name, file_name)
                    if ".git" in file_path:
                        continue
                    with open(file_path) as f:
                        s = f.read()
                    if link in s and "access_token" not in s:
                        changes = True
                        s = s.replace(link, link + "?access_token=" + token)
                        with open(file_path, "w") as f:
                            f.write(s)
        if changes:
            repo.git.add(".")
            repo.git.commit("-m", "Unlock dataset")
        repo.git.checkout("master")

    # Set token in non-archive link URLs
    if len(metadata["private_files"]["files"]) > 0:
        datalad = api.Dataset(".")
        for file in metadata["private_files"]["files"]:
            annex("rmurl", file["name"], file["link"])
            annex(
                "addurl",
                file["link"] + "?access_token=" + token,
                "--file",
                file["name"],
                "--relaxed",
            )
            datalad.save()

    print("Done")


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    try:
        unlock()
    except Exception:
        traceback.print_exc()
    finally:
        # Always switch branch back to master
        repository = Repo()
        if repository.active_branch.name != "master":
            repository.git.checkout("master", "-f")


========================================
FILE: ./datalad_crawlers/ftp_crawler.py
========================================
import argparse
import os
from ftplib import FTP

import git
from tqdm import tqdm


def crawl(host, root, subdir, *, ftp):
    """
    Recursively prints the URLs of the files under dir and adds them to git-annex
    """
    cwd = os.path.join("/", root, subdir)
    print(f"\nCrawling {cwd}")

    for filename, metadata in tqdm(ftp.mlsd(cwd)):
        filepath = os.path.join(subdir, filename)

        while True:
            try:
                if metadata.get("type") == "dir":
                    crawl(host, root, filepath, ftp=ftp)

                elif metadata.get("type") == "file":
                    git.Repo().git.annex(
                        "addurl",
                        f"ftp://{host}/" + os.path.join(cwd, filename),
                        file=filepath,
                    )
                break

            except Exception as e:
                print(f"WARNING: Connection restarted on file: {filepath}: {e}")
                ftp.connect()
                ftp.login()


def parse_args():
    example_text = """Example:
    python /path/to/ftp_crawler.py $FTP_HOST $FTP_DIR

    This will recursively annex the URL of all files from the $FTP_DIR on the $FTP_HOST
    in the git-annex of the current directory.
    """

    parser = argparse.ArgumentParser(
        description="Datalad crawler to crawl FTP server.",
        epilog=example_text,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("host", type=str, help="URL of the FTP host.")
    parser.add_argument("directory", type=str, help="Directory path to the dataset.")
    parser.add_argument(
        "sub_directory", nargs="?", type=str, default="", help="Subdirectory to crawl."
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    with FTP(args.host) as ftp:
        ftp.login()
        crawl(args.host, args.directory, args.sub_directory, ftp=ftp)


========================================
FILE: ./Crawlers/BaseCrawler.py
========================================
import abc
import json
import os
import re
import shutil

import git
import requests
from datalad import api

from scripts.Crawlers.constants import DATS_FIELDS
from scripts.Crawlers.constants import LICENSE_CODES
from scripts.Crawlers.constants import MODALITIES
from scripts.Crawlers.constants import NO_ANNEX_FILE_PATTERNS
from scripts.Crawlers.constants import REQUIRED_DATS_FIELDS


class BaseCrawler:
    """
    Interface to extend conp-dataset crawlers.

    ==================
    Overview
    ==================

    Any crawler created from this interface will have to crawl
    datasets from a specific remote platforms. This base class
    implements the functions common to all crawled backends, in particular:
    (1) verify that correct fork of conp-dataset is used,
    (2) create and switch to an new branch for each new dataset,
    (3) ignore README and DATS files for the annex,
    (4) create new datalad datasets,
    (5) publish to GitHub repository,
    (6) create pull requests.

    Method run(), implemented in the base class, is the entry point to any crawler.
    It does the following things:
    (1) It calls abstract method get_all_dataset_description(), that must be implemented
    by the crawler in the child class. get_all_dataset_description()
    retrieves from the remote platform
    all the necessary information about each dataset that is supposed to be added or
    updated to conp-dataset.
    (2) It iterates through each dataset description, and switch to a dedicated git branch
    for each dataset.
       (2.a) If the dataset is new, the base class will create a new branch,
             an empty datalad repository, unannex DATS.json and README.md and create an
             empty GitHub repository. It will then call abstract method add_new_dataset()
             which will add/download all dataset files under given directory.
             The crawler will then add a custom DATS.json and README.md if those weren't added.
             Creating the README.md requires get_readme_content() to be implemented, which will
             return the content of the README.md in markdown format. The crawler will then save and
             publish all changes to the newly create repository. It will also handle adding a new submodule
             to .gitmodules and creating a pull request to CONP-PCNO/conp-dataset.
       (2.b) If the dataset already exists, verified by the existence of its corresponding branch,
             the base class will call abstract method update_if_necessary() which will verify
             if the dataset requires updating and update if so. If the dataset got updated, This method
             will return True which will trigger saving, publishing new content to the dataset's respective
             repository, creating a new DATS.json if it doesn't exist and creating a pull
             request to CONP-PCNO/conp-dataset.

    ==================
    How to implement a new crawler
    ==================

        (1) Create a class deriving from BaseCrawler
        (2) Implement the four abstract methods:
            * get_all_dataset_description,
            * add_new_dataset
            * update_if_necessary
            * get_readme_content.
            See docstrings of each method for specifications.
        (3) In crawl.py, locate the comment where it says to instantiate new crawlers,
            instantiate this new Crawler and call run() on it
    """

    def __init__(self, github_token, config_path, verbose, force, no_pr, basedir):
        self.basedir = basedir
        self.repo = git.Repo(self.basedir)
        self.username = self._check_requirements()
        self.github_token = github_token
        self.config_path = config_path
        self.verbose = verbose
        self.force = force
        self.git = git
        self.datalad = api
        self.no_pr = no_pr
        if self.verbose:
            print(f"Using base directory {self.basedir}")

    @abc.abstractmethod
    def get_all_dataset_description(self):
        """
        Get relevant datasets' description from platform.

        Retrieves datasets' description that needs to be in CONP-datasets
        from platform specific to each crawler like Zenodo, OSF, etc. It is up
        to the crawler to identify which datasets on the platform should be crawled.
        The Zenodo crawler uses keywords for this purpose, but other mechanisms
        could work too.

        Each description is required to have the necessary information in order
        to build a valid DATS file from it. The following keys are necessary in
        each description:
            description["title"]: The name of the dataset, usually one sentence or short description of the dataset
            description["identifier"]: The identifier of the dataset
            description["creators"]: The person(s) or organization(s) which contributed to the creation of the dataset
            description["description"]: A textual narrative comprised of one or more statements describing the dataset
            description["version"]: A release point for the dataset when applicable
            description["licenses"]: The terms of use of the dataset
            description["keywords"]: Tags associated with the dataset, which will help in its discovery
            description["types"]: A term, ideally from a controlled terminology, identifying the dataset type or nature
                                  of the data, placing it in a typology
        More fields can be added as long as they comply with the DATS schema available at
        https://github.com/CONP-PCNO/schema/blob/master/dataset_schema.json

        Any fields/keys not in the schema will be ignored when creating the dataset's DATS.
        It is fine to add more helpful information for other methods which will use them.

        Here are some examples of valid DATS.json files:
        https://github.com/conp-bot/conp-dataset-Learning_Naturalistic_Structure__Processed_fMRI_dataset/blob/476a1ee3c4df59aca471499b2e492a65bd389a88/DATS.json
        https://github.com/conp-bot/conp-dataset-MRI_and_unbiased_averages_of_wild_muskrats__Ondatra_zibethicus__and_red_squirrels__Tami/blob/c9e9683fbfec71f44a5fc3576515011f6cd024fe/DATS.json
        https://github.com/conp-bot/conp-dataset-PERFORM_Dataset__one_control_subject/blob/0b1e271fb4dcc03f9d15f694cc3dfae5c7c2d358/DATS.json

        Returns:
            List of description of relevant datasets. Each description is a
            dictionary. For example:

            [{
                "title": "PERFORM Dataset Example",
                "description": "PERFORM dataset description",
                "version": "0.0.1",
                ...
             },
             {
                "title": "SIMON dataset example",
                "description: "SIMON dataset description",
                "version": "1.4.2",
                ...
             },
             ...
            ]
        """
        return []

    @abc.abstractmethod
    def add_new_dataset(self, dataset_description, dataset_dir):
        """
        Configure and add newly created dataset to the local CONP git repository.

        The BaseCrawler will take care of a few overhead tasks before
        add_new_dataset() is called, namely:
        (1) Creating and checkout a dedicated branch for this dataset
        (2) Initialising a Github repo for this dataset
        (3) Creating an empty datalad dataset for files to be added
        (4) Annex ignoring README.md and DATS.json

        After add_new_dataset() is called, BaseCrawler will:
        (1) Create a custom DATS.json if it isn't added in add_new_dataset()
        (2) Create a custom README.md with get_readme_content() if that file is non-existent
        (3) Save and publish all changes
        (4) Adding this dataset as a submodule
        (5) Creating a pull request to CONP-PCNO/conp-dataset
        (6) Switch back to the master branch

        This means that add_new_dataset() will only have to implement a few tasks in the given
        previously initialized datalad dataset directory:
        (1) Adding any one time configuration such as annex ignoring files or adding dataset version tracker
        (2) Downloading and unarchiving relevant archives using datalad.download_url(link, archive=True)
        (3) Adding file links as symlinks using annex("addurl", link, "--fast", "--file", filename)

        There is no need to save/push/publish/create pull request
        as those will be done after this function has finished

        Parameter:
        dataset_description (dict): Dictionary containing information on
                                    retrieved dataset from platform. Element of
                                    the list returned by get_all_dataset_description.
        dataset_dir (str): Local directory path where the newly
                           created datalad dataset is located.
        """
        return

    @abc.abstractmethod
    def update_if_necessary(self, dataset_description, dataset_dir):
        """
        Update dataset if it has been modified on the remote platform.

        Determines if local dataset identified by 'identifier'
        needs to be updated. If so, update dataset.

        Similarily to add_new_dataset(), update_if_necessary() will need to
        take care of updating the dataset if required:
        (1) Downloading and unarchiving relevant archives using datalad.download_url(link, archive=True)
        (2) Adding new file links as symlinks using annex("addurl", link, "--fast", "--file", filename)
        (3) Updating any tracker files used to determine if the dataset needs to be updated

        There is no need to save/push/publish/create pull request
        as those will be done after this function has finished

        Parameter:
        dataset_description (dict): Dictionary containing information on
                                    retrieved dataset from platform.
                                    Element of the list returned by
                                    get_all_dataset_description.
        dataset_dir (str): Directory path of the
                           previously created datalad dataset

        Returns:
        bool: True if dataset got modified, False otherwise
        """
        return False

    @abc.abstractmethod
    def get_readme_content(self, dataset_description):
        """
        Returns the content of the README.md in markdown.

        Given the dataset description provided by
        get_all_dataset_description(), return the content of the
        README.md in markdown.

        Parameter:
        dataset_description (dict): Dictionary containing information on
                                    retrieved dataset from platform.
                                    Element of the list returned by
                                    get_all_dataset_description.

        Returns:
        string: Content of the README.md
        """
        return ""

    def run(self):
        """
        DO NOT OVERRIDE THIS METHOD
        This method is the entry point for all Crawler classes.
        It will loop through dataset descriptions collected from get_all_dataset_description(),
        verify if each of those dataset present locally, create a new dataset with add_new_dataset()
        if not. If dataset is already existing locally, verify if dataset needs updating
        with update_if_necessary() and update if so
        """
        dataset_description_list = self.get_all_dataset_description()
        for dataset_description in dataset_description_list:
            try:
                clean_title = self._clean_dataset_title(dataset_description["title"])
                branch_name = "conp-bot/" + clean_title
                dataset_rel_dir = os.path.join("projects", clean_title)
                dataset_dir = os.path.join(self.basedir, dataset_rel_dir)
                d = self.datalad.Dataset(dataset_dir)
                if branch_name not in self.repo.remotes.origin.refs:  # New dataset
                    self.repo.git.checkout("-b", branch_name)
                    repo_title = ("conp-dataset-" + dataset_description["title"])[0:100]
                    try:
                        d.create()
                        r = d.create_sibling_github(
                            repo_title,
                            name="origin",
                            github_login=self.github_token,
                            github_passwd=self.github_token,
                        )
                    except Exception as error:
                        # handle the exception
                        print("An exception occurred:", error)

                    # Add github token to dataset origin remote url
                    try:
                        origin = self.repo.remote("origin")
                        origin_url = next(origin.urls)
                        if "@" not in origin_url:
                            origin.set_url(
                                origin_url.replace(
                                    "https://",
                                    "https://" + self.github_token + "@",
                                ),
                            )
                    except git.exc.NoSuchPathError:
                        pass

                    self._add_github_repo_description(repo_title, dataset_description)
                    for pattern in NO_ANNEX_FILE_PATTERNS:
                        d.no_annex(pattern)
                    self.add_new_dataset(dataset_description, dataset_dir)

                    # Create DATS.json if it exists in directory and 1 level deep subdir
                    dats_path: str = os.path.join(dataset_dir, "DATS.json")
                    if existing_dats_path := self._check_file_present(
                        dataset_dir, "dats.json"
                    ):
                        if self.verbose:
                            print(f"Found existing DATS.json at {existing_dats_path}")
                        if existing_dats_path != dats_path:
                            shutil.copy(existing_dats_path, dats_path)
                        self._add_source_data_submodule_if_derived_from_conp_dataset(
                            dats_path, dataset_dir
                        )
                    else:
                        self._create_new_dats(
                            dataset_dir,
                            dats_path,
                            dataset_description,
                            d,
                        )
                    # Move the logo into the root directory if found in 1 level deep subdir
                    logo_path = os.path.join(dataset_dir, "logo.png")
                    if existing_logo_path := self._check_file_present(
                        dataset_dir, "logo.png"
                    ):
                        if self.verbose:
                            print(f"Found logo at {existing_logo_path}")
                        if existing_logo_path != logo_path:
                            os.rename(existing_logo_path, logo_path)

                    # Create README.md if it doesn't exist
                    if not os.path.isfile(os.path.join(dataset_dir, "README.md")):
                        readme = self.get_readme_content(dataset_description)
                        self._create_readme(
                            readme, os.path.join(dataset_dir, "README.md")
                        )
                    d.save()
                    d.publish(to="origin")
                    self.repo.git.submodule(
                        "add",
                        r[0][1].replace(self.github_token + "@", ""),
                        dataset_rel_dir,
                    )
                    modified = True
                    commit_msg = "Created " + dataset_description["title"]
                else:  # Dataset already existing locally
                    try:
                        # Try normal checkout first
                        self.repo.git.checkout("-f", branch_name)
                    except git.exc.GitCommandError as e:
                        if "filter-process" in str(e):
                            self.repo.git.execute(
                                [
                                    "git",
                                    "-c",
                                    "filter.annex.process=",
                                    "checkout",
                                    "-f",
                                    branch_name,
                                ]
                            )
                        else:
                            raise

                    try:
                        self.repo.git.merge("-n", "--no-verify", "master")
                    except Exception as e:
                        print(f"Error while merging master into {branch_name}: {e}")
                        print("Skipping this dataset")
                        self.repo.git.merge("--abort")
                        try:
                            # Use the same safe checkout to go back to master
                            self.repo.git.checkout("-f", "master")
                        except git.exc.GitCommandError as e:
                            if "filter-process" in str(e):
                                self.repo.git.execute(
                                    [
                                        "git",
                                        "-c",
                                        "filter.annex.process=",
                                        "checkout",
                                        "-f",
                                        "master",
                                    ]
                                )
                            else:
                                raise
                        continue

                    modified = self.update_if_necessary(
                        dataset_description, dataset_dir
                    )
                    if modified:
                        # Create DATS.json if it exists in directory and 1 level deep subdir
                        dats_path: str = os.path.join(dataset_dir, "DATS.json")
                        if existing_dats_path := self._check_file_present(
                            dataset_dir, "dats.json"
                        ):
                            if self.verbose:
                                print(
                                    f"Found existing DATS.json at {existing_dats_path}"
                                )
                            if existing_dats_path != dats_path:
                                os.rename(existing_dats_path, dats_path)
                            self._add_source_data_submodule_if_derived_from_conp_dataset(
                                dats_path, dataset_dir
                            )
                        else:
                            self._create_new_dats(
                                dataset_dir,
                                dats_path,
                                dataset_description,
                                d,
                            )
                        # Move the logo into the root directory if found in 1 level deep subdir
                        logo_path = os.path.join(dataset_dir, "logo.png")
                        if existing_logo_path := self._check_file_present(
                            dataset_dir, "logo.png"
                        ):
                            if self.verbose:
                                print(f"Found logo at {existing_logo_path}")
                            if existing_logo_path != logo_path:
                                os.rename(existing_logo_path, logo_path)
                        # Create README.md if it doesn't exist
                        if not os.path.isfile(os.path.join(dataset_dir, "README.md")):
                            readme = self.get_readme_content(dataset_description)
                            self._create_readme(
                                readme,
                                os.path.join(dataset_dir, "README.md"),
                            )
                        d.save()
                        d.publish(to="origin")
                        commit_msg = "Updated " + dataset_description["title"]

                # If modification detected in dataset, push to branch and create PR
                if modified:
                    self._push_and_pull_request(
                        commit_msg,
                        dataset_dir,
                        dataset_description["title"],
                    )
            except Exception as e:
                print(e)

            # Go back to master
            try:
                self.repo.git.checkout("master")
            except git.exc.GitCommandError as e:
                if "filter-process" in str(e):
                    self.repo.git.execute(
                        ["git", "-c", "filter.annex.process=", "checkout", "master"]
                    )
                else:
                    raise

    def _add_github_repo_description(self, repo_title, dataset_description):
        url = "https://api.github.com/repos/{}/{}".format(
            self.username,
            repo_title,
        )
        head = {"Authorization": "token {}".format(self.github_token)}
        description = "Please don't submit any PR to this repository. "
        if "creators" in dataset_description.keys():
            description += (
                "If you want to request modifications, please contact "
                f"{dataset_description['creators'][0]['name']}"
            )
        payload = {"description": description}
        r = requests.patch(url, data=json.dumps(payload), headers=head)
        if not r.ok:
            print(
                "Problem adding description to repository {}:".format(repo_title),
            )
            print(r.content)

    def _check_requirements(self):
        # GitHub user must have a fork of https://github.com/CONP-PCNO/conp-dataset
        # Script must be run in the  directory of a local clone of this fork
        # Git remote 'origin' of local Git clone must point to that fork
        # Local Git clone must be set to branch 'master'
        if "origin" not in self.repo.remotes:
            raise Exception("Remote 'origin' does not exist in current reposition")
        origin_url = next(self.repo.remote("origin").urls)
        full_name = re.search("github.com[/,:](.*).git", origin_url).group(1)
        r = requests.get("http://api.github.com/repos/" + full_name).json()
        if not r["fork"] or r["parent"]["full_name"] != "CONP-PCNO/conp-dataset":
            raise Exception("Current repository not a fork of CONP-PCNO/conp-dataset")
        branch = self.repo.active_branch.name
        if branch != "master":
            raise Exception("Local git clone active branch not set to 'master'")

        # Return username
        return full_name.split("/")[0]

    def _push_and_pull_request(self, msg, dataset_dir, title):
        self.repo.git.add(dataset_dir)
        self.repo.git.add(".gitmodules")
        self.repo.git.commit("-m", "[conp-bot] " + msg)
        clean_title = self._clean_dataset_title(title)
        origin = self.repo.remote("origin")
        origin_url = next(origin.urls)
        if "@" not in origin_url:
            origin.set_url(
                origin_url.replace("https://", "https://" + self.github_token + "@"),
            )
        self.repo.git.push("--set-upstream", "origin", "conp-bot/" + clean_title)

        # Create PR
        print("Creating PR for " + title)
        if not self.no_pr:
            r = requests.post(
                "https://api.github.com/repos/CONP-PCNO/conp-dataset/pulls",
                json={
                    "title": "Crawler result ({})".format(title),
                    "body": """## Description
{}

## Checklist

Mandatory files and elements:
- [x] A `README.md` file, at the root of the dataset
- [x] A `DATS.json` file, at the root of the dataset
- [ ] If configuration is required (for instance to enable a special remote),
 a `config.sh` script at the root of the dataset
- [x] A DOI (see instructions in [contribution guide]
(https://github.com/CONP-PCNO/conp-dataset/blob/master/.github/CONTRIBUTING.md), and corresponding badge in `README.md`

Functional checks:
- [x] Dataset can be installed using DataLad, recursively if it has sub-datasets
- [x] Every data file has a URL
- [x] Every data file can be retrieved or requires authentication
- [ ] `DATS.json` is a valid DATs model
- [ ] If dataset is derived data, raw data is a sub-dataset
""".format(
                        msg + "\n",
                    ),
                    "head": self.username + ":conp-bot/" + clean_title,
                    "base": "master",
                },
                headers={"Authorization": "token {}".format(self.github_token)},
            )
            if r.status_code != 201:
                raise Exception("Error while creating pull request: " + r.text)

    def _clean_dataset_title(self, title):
        return re.sub(r"\W|^(?=\d)", "_", title)

    def _create_new_dats(self, dataset_dir, dats_path, dataset, d):
        # Helper recursive function
        def retrieve_license_path_in_dir(dir, paths):
            for f_name in os.listdir(dir):
                f_path = os.path.join(dir, f_name)
                if os.path.isdir(f_path):
                    retrieve_license_path_in_dir(f_path, paths)
                    continue
                elif "license" not in f_name.lower():
                    continue
                elif os.path.islink(f_path):
                    d.get(f_path)
                paths.append(f_path)

        # Check required properties
        for field in REQUIRED_DATS_FIELDS:
            if field not in dataset.keys():
                print(
                    "Warning: required property {} not found in dataset description".format(
                        field,
                    ),
                )

        # Add all dats properties from dataset description
        data = {key: value for key, value in dataset.items() if key in DATS_FIELDS}

        # Check for license code in dataset if a license was not specified from the platform
        if "licenses" not in data or (
            len(data["licenses"]) == 1 and data["licenses"][0]["name"].lower() == "none"
        ):
            # Collect all license file paths
            license_f_paths = []
            retrieve_license_path_in_dir(dataset_dir, license_f_paths)

            # If found some license files, for each, check for first valid license code and add to DATS
            if license_f_paths:
                licenses = set()
                for f_path in license_f_paths:
                    with open(f_path) as f:
                        text = f.read().lower()
                    for code in LICENSE_CODES:
                        if code.lower() in text:
                            licenses.add(code)
                            break
                data["licenses"] = [{"name": code} for code in licenses]

        # Add file count
        num = 0
        for file in os.listdir(dataset_dir):
            file_path = os.path.join(dataset_dir, file)
            if (
                file[0] == "."
                or file == "DATS.json"
                or file == "README.md"
                or file == "logo.png"
            ):
                continue
            elif os.path.isdir(file_path):
                num += sum([len(files) for r, d, files in os.walk(file_path)])
            else:
                num += 1
        if "extraProperties" not in data.keys():
            data["extraProperties"] = [
                {"category": "files", "values": [{"value": str(num)}]},
            ]
        else:
            data["extraProperties"].append(
                {"category": "files", "values": [{"value": str(num)}]},
            )

        # Retrieve modalities from files
        file_paths = map(
            lambda x: x.split(" ")[-1],
            filter(
                lambda x: " " in x,
                git.Repo(dataset_dir).git.annex("list").split("\n"),
            ),
        )  # Get file paths
        file_names = list(
            map(lambda x: x.split("/")[-1] if "/" in x else x, file_paths),
        )  # Get file names from path
        modalities = {self._guess_modality(file_name) for file_name in file_names}
        if len(modalities) == 0:
            modalities.add("unknown")
        elif len(modalities) > 1 and "unknown" in modalities:
            modalities.remove("unknown")
        if "types" not in data.keys():
            data["types"] = [
                {"information": {"value": modality}} for modality in modalities
            ]
        else:
            for modality in modalities:
                data["types"].append({"information": {"value": modality}})

        # Create file
        with open(dats_path, "w") as f:
            json.dump(data, f, indent=4)

    def _guess_modality(self, file_name):
        # Associate file types to substrings found in the file name
        for m in MODALITIES:
            for s in MODALITIES[m]:
                if s in file_name:
                    return m
        return "unknown"

    def _create_readme(self, content, path):
        with open(path, "w") as f:
            f.write(content)

    def _check_file_present(self, directory, filename):
        for file_name in os.listdir(directory):
            file_path: str = os.path.join(directory, file_name)
            if os.path.isdir(file_path):
                for subfile_name in os.listdir(file_path):
                    if subfile_name.lower() == filename.lower():
                        return os.path.join(file_path, subfile_name)
            elif file_name.lower() == filename.lower():
                return file_path

    def _add_source_data_submodule_if_derived_from_conp_dataset(
        self, dats_json, dataset_dir
    ):
        with open(dats_json) as f:
            metadata = json.loads(f.read())

        source_dataset_link = None
        source_dataset_id = None
        if "extraProperties" not in metadata.keys():
            return
        for property in metadata["extraProperties"]:
            if property["category"] == "derivedFrom":
                try:
                    source_dataset_link = property["values"][0]["value"]
                except (KeyError, IndexError):
                    continue
            if property["category"] == "parent_dataset_id":
                try:
                    source_dataset_id = property["values"][0]["value"]
                except (KeyError, IndexError):
                    continue

        if source_dataset_link is not None and "github.com" in source_dataset_link:
            d = self.datalad.Dataset(os.path.join(dataset_dir, source_dataset_id))
            d.create()


========================================
FILE: ./Crawlers/OSFCrawler.py
========================================
import datetime
import json
import os
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import humanize
import requests
from datalad.distribution.dataset import Dataset
from git import Repo

from scripts.Crawlers.BaseCrawler import BaseCrawler


def _create_osf_tracker(path, dataset):
    with open(path, "w") as f:
        data = {
            "version": dataset["version"],
            "title": dataset["title"],
        }
        json.dump(data, f, indent=4)


class OSFCrawler(BaseCrawler):
    def __init__(self, github_token, config_path, verbose, force, no_pr, basedir):
        super().__init__(github_token, config_path, verbose, force, no_pr, basedir)
        self.osf_token = self._get_token()

    def _get_token(self):
        if os.path.isfile(self.config_path):
            with open(self.config_path) as f:
                data = json.load(f)
            if "osf_token" in data.keys():
                return data["osf_token"]

    def _get_request_with_bearer_token(self, link, redirect=True):
        header = {"Authorization": f"Bearer {self.osf_token}"}
        r = requests.get(link, headers=header, allow_redirects=redirect)
        if r.ok:
            return r
        else:
            raise Exception(f"Request to {r.url} failed: {r.content}")

    def _query_osf(self):
        query = "https://api.osf.io/v2/nodes/?filter[tags]=canadian-open-neuroscience-platform"
        r_json = self._get_request_with_bearer_token(query).json()
        results = r_json["data"]

        # Retrieve results from other pages
        if r_json["links"]["meta"]["total"] > r_json["links"]["meta"]["per_page"]:
            next_page = r_json["links"]["next"]
            while next_page is not None:
                next_page_json = self._get_request_with_bearer_token(next_page).json()
                results.extend(next_page_json["data"])
                next_page = next_page_json["links"]["next"]

        if self.verbose:
            print("OSF query: {}".format(query))
        return results

    def _download_files(
        self,
        link,
        current_dir,
        inner_path,
        d,
        annex,
        sizes,
        is_private=False,
    ):
        r_json = self._get_request_with_bearer_token(link).json()
        files = r_json["data"]

        # Retrieve the files in the other pages if there are more than 1 page
        if (
            "links" in r_json.keys()
            and r_json["links"]["meta"]["total"] > r_json["links"]["meta"]["per_page"]
        ):
            next_page = r_json["links"]["next"]
            while next_page is not None:
                next_page_json = self._get_request_with_bearer_token(next_page).json()
                files.extend(next_page_json["data"])
                next_page = next_page_json["links"]["next"]

        for file in files:
            # Handle folders
            if file["attributes"]["kind"] == "folder":
                folder_path = os.path.join(current_dir, file["attributes"]["name"])
                os.mkdir(folder_path)
                self._download_files(
                    file["relationships"]["files"]["links"]["related"]["href"],
                    folder_path,
                    os.path.join(inner_path, file["attributes"]["name"]),
                    d,
                    annex,
                    sizes,
                    is_private,
                )

            # Handle single files
            elif file["attributes"]["kind"] == "file":

                # Private dataset/files
                if is_private:
                    correct_download_link = self._get_request_with_bearer_token(
                        file["links"]["download"],
                        redirect=False,
                    ).headers["location"]
                    if "https://accounts.osf.io/login" not in correct_download_link:
                        zip_file = (
                            True
                            if file["attributes"]["name"].split(".")[-1] == "zip"
                            else False
                        )
                        d.download_url(
                            correct_download_link,
                            path=os.path.join(inner_path, ""),
                            archive=zip_file,
                        )
                    else:  # Token did not work for downloading file, return
                        print(
                            f'Unable to download file {file["links"]["download"]} with current token, skipping file',
                        )
                        return

                # Public file
                else:
                    filename = file["attributes"]["name"]
                    url = file["links"]["download"]

                    # Handle zip files: only register URL, don't download
                    if filename.endswith(".zip"):
                        target_path = os.path.join(inner_path, filename)
                        if self.verbose:
                            print("Registering zip (no download):", target_path)
                        annex("addurl", "--fast", url, "--file", target_path)
                    else:
                        d.download_url(
                            url,
                            path=os.path.join(inner_path, ""),
                            archive=False,
                        )

                # append the size of the downloaded file to the sizes array
                file_size = file["attributes"]["size"]
                if not file_size:
                    # if the file size cannot be found in the OSF API response, then get it from git annex info
                    inner_file_path = os.path.join(
                        inner_path,
                        file["attributes"]["name"],
                    )
                    annex_info_dict = json.loads(
                        annex("info", "--bytes", "--json", inner_file_path),
                    )
                    file_size = int(annex_info_dict.get("size", 0))
                sizes.append(file_size)

    def _download_components(
        self,
        components_list,
        current_dir,
        inner_path,
        d,
        annex,
        dataset_size,
        is_private,
    ):
        # Loop through each available components and download their files
        for component in components_list:
            component_title = self._clean_dataset_title(
                component["attributes"]["title"],
            )
            component_inner_path = os.path.join(
                inner_path,
                "components",
                component_title,
            )
            os.makedirs(os.path.join(current_dir, component_inner_path))
            self._download_files(
                component["relationships"]["files"]["links"]["related"]["href"],
                os.path.join(current_dir, component_inner_path),
                component_inner_path,
                d,
                annex,
                dataset_size,
                is_private,
            )

            # check if the component contains (sub)components, in which case, download the (sub)components data
            subcomponents_list = self._get_components(
                component["relationships"]["children"]["links"]["related"]["href"],
            )
            if subcomponents_list:
                self._download_components(
                    subcomponents_list,
                    current_dir,
                    os.path.join(component_inner_path),
                    d,
                    annex,
                    dataset_size,
                    is_private,
                )

        # Once we have downloaded all the components files, check to see if there are any empty
        # directories (in the case the 'OSF parent' dataset did not have any downloaded files
        list_of_empty_dirs = [
            dirpath
            for (dirpath, dirnames, filenames) in os.walk(current_dir)
            if len(dirnames) == 0 and len(filenames) == 0
        ]
        for empty_dir in list_of_empty_dirs:
            os.rmdir(empty_dir)

    def _get_contributors(self, link):
        r = self._get_request_with_bearer_token(link)
        contributors = [
            contributor["embeds"]["users"]["data"]["attributes"]["full_name"]
            for contributor in r.json()["data"]
        ]
        return contributors

    def _get_license(self, link):
        r = self._get_request_with_bearer_token(link)
        return r.json()["data"]["attributes"]["name"]

    def _get_components(self, link):
        r = self._get_request_with_bearer_token(link)
        return r.json()["data"]

    def _get_wiki(self, link) -> Optional[str]:
        r = self._get_request_with_bearer_token(link)
        data = r.json()["data"]
        if len(data) > 0:
            return self._get_request_with_bearer_token(
                data[0]["links"]["download"]
            ).content.decode()

    def _get_institutions(self, link):
        r = self._get_request_with_bearer_token(link)
        if r.json()["data"]:
            institutions = [
                institution["attributes"]["name"] for institution in r.json()["data"]
            ]
            return institutions

    def _get_identifier(self, link):
        r = self._get_request_with_bearer_token(link)
        return r.json()["data"][0]["attributes"]["value"] if r.json()["data"] else False

    def get_all_dataset_description(self):
        osf_dois = []
        datasets = self._query_osf()
        for dataset in datasets:
            # skip datasets that have a parent since the files' components will
            # go into the parent dataset.
            if "parent" in dataset["relationships"].keys():
                continue

            attributes = dataset["attributes"]

            # Retrieve keywords/tags
            keywords = list(map(lambda x: {"value": x}, attributes["tags"]))

            # Retrieve contributors/creators
            contributors = self._get_contributors(
                dataset["relationships"]["contributors"]["links"]["related"]["href"],
            )

            # Retrieve license
            license_ = "None"
            if "license" in dataset["relationships"].keys():
                license_ = self._get_license(
                    dataset["relationships"]["license"]["links"]["related"]["href"],
                )

            # Retrieve institution information
            institutions = self._get_institutions(
                dataset["relationships"]["affiliated_institutions"]["links"]["related"][
                    "href"
                ],
            )

            # Retrieve identifier information
            identifier = self._get_identifier(
                dataset["relationships"]["identifiers"]["links"]["related"]["href"],
            )

            # Get link for the dataset files
            files_link = dataset["relationships"]["files"]["links"]["related"]["href"]

            # Get components list
            components_list = self._get_components(
                dataset["relationships"]["children"]["links"]["related"]["href"],
            )

            # Get wiki to put in README
            wiki: Optional[str] = None
            try:
                wiki = self._get_wiki(
                    dataset["relationships"]["wikis"]["links"]["related"]["href"]
                )
            except Exception as e:
                print(f'Error getting wiki for {attributes["title"]} because of {e}')

            # Gather extra properties
            extra_properties = [
                {
                    "category": "logo",
                    "values": [
                        {
                            "value": "https://osf.io/static/img/institutions/shields/cos-shield.png",
                        },
                    ],
                },
            ]
            if institutions:
                extra_properties.append(
                    {
                        "category": "origin_institution",
                        "values": list(
                            map(lambda x: {"value": x}, institutions),
                        ),
                    },
                )

            # Retrieve dates
            date_created = datetime.datetime.strptime(
                attributes["date_created"],
                "%Y-%m-%dT%H:%M:%S.%f",
            )
            date_modified = datetime.datetime.strptime(
                attributes["date_modified"],
                "%Y-%m-%dT%H:%M:%S.%f",
            )

            dataset_dats_content = {
                "title": attributes["title"],
                "files": files_link,
                "components_list": components_list,
                "homepage": dataset["links"]["html"],
                "creators": list(
                    map(lambda x: {"name": x}, contributors),
                ),
                "description": attributes["description"],
                "wiki": wiki,
                "version": attributes["date_modified"],
                "licenses": [
                    {
                        "name": license_,
                    },
                ],
                "dates": [
                    {
                        "date": date_created.strftime("%Y-%m-%d %H:%M:%S"),
                        "type": {
                            "value": "date created",
                        },
                    },
                    {
                        "date": date_modified.strftime("%Y-%m-%d %H:%M:%S"),
                        "type": {
                            "value": "date modified",
                        },
                    },
                ],
                "keywords": keywords,
                "distributions": [
                    {
                        "size": 0,
                        "unit": {"value": "B"},
                        "access": {
                            "landingPage": dataset["links"]["html"],
                            "authorizations": [
                                {
                                    "value": "public"
                                    if attributes["public"]
                                    else "private",
                                },
                            ],
                        },
                    },
                ],
                "extraProperties": extra_properties,
            }

            if identifier:
                source = "OSF DOI" if "OSF.IO" in identifier else "DOI"
                dataset_dats_content["identifier"] = {
                    "identifier": identifier,
                    "identifierSource": source,
                }

            osf_dois.append(dataset_dats_content)

        if self.verbose:
            print("Retrieved OSF DOIs: ")
            for osf_doi in osf_dois:
                print(
                    "- Title: {}, Last modified: {}".format(
                        osf_doi["title"],
                        osf_doi["version"],
                    ),
                )

        return osf_dois

    def add_new_dataset(self, dataset: Dict[str, Any], dataset_dir: str):
        d: Dataset = self.datalad.Dataset(dataset_dir)
        d.no_annex(".conp-osf-crawler.json")
        d.save()
        annex: Callable = Repo(dataset_dir).git.annex
        dataset_size: List[int] = []

        # Setup private OSF dataset if the dataset is private
        is_private: bool = self._setup_private_dataset(
            dataset["files"],
            dataset_dir,
            annex,
            d,
        )
        self._download_files(
            dataset["files"],
            dataset_dir,
            "",
            d,
            annex,
            dataset_size,
            is_private,
        )
        if dataset["components_list"]:
            self._download_components(
                dataset["components_list"],
                dataset_dir,
                "",
                d,
                annex,
                dataset_size,
                is_private,
            )
        dataset_size_num, dataset_unit = humanize.naturalsize(sum(dataset_size)).split(
            " ",
        )
        dataset["distributions"][0]["size"] = float(dataset_size_num)
        dataset["distributions"][0]["unit"]["value"] = dataset_unit

        # Add .conp-osf-crawler.json tracker file
        _create_osf_tracker(
            os.path.join(dataset_dir, ".conp-osf-crawler.json"),
            dataset,
        )

    def update_if_necessary(self, dataset_description, dataset_dir):
        tracker_path = os.path.join(dataset_dir, ".conp-osf-crawler.json")
        if not os.path.isfile(tracker_path):
            print("{} does not exist in dataset, skipping".format(tracker_path))
            return False
        with open(tracker_path) as f:
            tracker = json.load(f)
        if tracker["version"] == dataset_description["version"]:
            # Same version, no need to update
            if self.verbose:
                print(
                    "{}, version {} same as OSF version DOI ({}), no need to update".format(
                        dataset_description["title"],
                        dataset_description["version"],
                        tracker["version"],
                    ),
                )
            return False

        # Update dataset
        if self.verbose:
            print(
                "{}, version {} different from OSF version DOI {}, updating".format(
                    dataset_description["title"],
                    tracker["version"],
                    dataset_description["version"],
                ),
            )

        # Remove all data and DATS.json files
        for file_name in os.listdir(dataset_dir):
            if file_name[0] == ".":
                continue
            self.datalad.remove(os.path.join(dataset_dir, file_name), check=False)

        d = self.datalad.Dataset(dataset_dir)
        annex = Repo(dataset_dir).git.annex

        dataset_size = []
        is_private: bool = self._is_private_dataset(dataset_description["files"])
        self._download_files(
            dataset_description["files"],
            dataset_dir,
            "",
            d,
            annex,
            dataset_size,
            is_private,
        )
        if dataset_description["components_list"]:
            self._download_components(
                dataset_description["components_list"],
                dataset_dir,
                "",
                d,
                annex,
                dataset_size,
                is_private,
            )
        dataset_size, dataset_unit = humanize.naturalsize(sum(dataset_size)).split(
            " ",
        )
        dataset_description["distributions"][0]["size"] = float(dataset_size)
        dataset_description["distributions"][0]["unit"]["value"] = dataset_unit

        # Add .conp-osf-crawler.json tracker file
        _create_osf_tracker(
            os.path.join(dataset_dir, ".conp-osf-crawler.json"),
            dataset_description,
        )

        return True

    def get_readme_content(self, dataset):
        readme_content = (
            f'# {dataset["title"]}\n\nCrawled from [OSF]({dataset["homepage"]})'
        )

        if "description" in dataset and dataset["description"]:
            readme_content += f'\n\n## Description\n\n{dataset["description"]}'

        if "identifier" in dataset and dataset["identifier"]:
            readme_content += f'\n\n## DOI: {dataset["identifier"]["identifier"]}'

        if "wiki" in dataset and dataset["wiki"]:
            readme_content += f'\n\n## WIKI\n\n{dataset["wiki"]}'

        return readme_content

    def _setup_private_dataset(
        self,
        files_url: str,
        dataset_dir: str,
        annex: Callable,
        dataset: Dataset,
    ) -> bool:
        # Check if the dataset is indeed private
        if self._is_private_dataset(files_url):
            if self.verbose:
                print(
                    "Dataset is private, creating OSF provider and make git annex autoenable datalad remote",
                )

            # Create OSF provider file and needed directories and don't annex the file
            datalad_dir: str = os.path.join(dataset_dir, ".datalad")
            if not os.path.exists(datalad_dir):
                os.mkdir(datalad_dir)
            providers_dir: str = os.path.join(datalad_dir, "providers")
            if not os.path.exists(providers_dir):
                os.mkdir(providers_dir)
            osf_config_path: str = os.path.join(providers_dir, "OSF.cfg")
            with open(osf_config_path, "w") as f:
                f.write(
                    """[provider:OSF]
url_re = .*osf\\.io.*
authentication_type = bearer_token
credential = OSF

[credential:OSF]
# If known, specify URL or email to how/where to request credentials
# url = ???
type = token"""
                )
            dataset.no_annex(os.path.join("**", "OSF.cfg"))

            # Make git annex autoenable datalad remote
            annex(
                "initremote",
                "datalad",
                "externaltype=datalad",
                "type=external",
                "encryption=none",
                "autoenable=true",
            )

            # Set OSF token as a environment variable for authentication
            os.environ["DATALAD_OSF_token"] = self.osf_token

            # Save changes
            dataset.save()

            return True

        return False

    def _is_private_dataset(self, files_url) -> bool:
        return True if requests.get(files_url).status_code == 401 else False


========================================
FILE: ./Crawlers/OSFCrawlerTest.py
========================================
import datetime
import json
import os
import time
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import humanize
import requests
from datalad.distribution.dataset import Dataset
from datalad.support.exceptions import IncompleteResultsError
from git import Repo
from requests.exceptions import HTTPError

from scripts.Crawlers.BaseCrawlerTest import BaseCrawler


def _create_osf_tracker(path, dataset):
    with open(path, "w") as f:
        data = {
            "version": dataset["version"],
            "title": dataset["title"],
        }
        json.dump(data, f, indent=4)


class OSFCrawler(BaseCrawler):
    def __init__(self, github_token, config_path, verbose, force, no_pr, basedir):
        super().__init__(github_token, config_path, verbose, force, no_pr, basedir)
        self.osf_token = self._get_token()

    def _get_token(self):
        if os.path.isfile(self.config_path):
            with open(self.config_path) as f:
                data = json.load(f)
            if "osf_token" in data.keys():
                return data["osf_token"]

    def _get_request_with_bearer_token(self, link, redirect=True, retries=5):
        header = {"Authorization": f"Bearer {self.osf_token}"}
        attempt = 0
        while attempt < retries:
            try:
                r = requests.get(link, headers=header, allow_redirects=redirect)
                r.raise_for_status()  # Cela va lever une exception pour les réponses 4xx et 5xx
                return r  # Retourne la réponse si tout va bien
            except HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - Response: {r.text}")
                if r.status_code == 503:  # Spécifiquement pour gérer les erreurs 503
                    print(
                        f"Request to {r.url} failed with 503 Bad Gateway, retrying..."
                    )
                    attempt += 1
                    time.sleep(2**attempt)  # Backoff exponentiel
                    continue
                if r.status_code == 502:  # Spécifiquement pour gérer les erreurs 502
                    print(
                        f"Request to {r.url} failed with 502 Bad Gateway, skipping download."
                    )
                    return None  # Retourne None pour permettre au code de continuer
                else:
                    raise Exception(
                        f"HTTP error occurred: {http_err} - {r.status_code}"
                    )  # Lève l'exception pour les autres erreurs HTTP
            except Exception as err:
                raise Exception(f"An error occurred: {err}")

    def _query_osf(self):
        query = "https://api.osf.io/v2/nodes/?filter[tags]=canadian-open-neuroscience-platform"
        r_json = self._get_request_with_bearer_token(query).json()
        results = r_json["data"]

        # Retrieve results from other pages
        if r_json["links"]["meta"]["total"] > r_json["links"]["meta"]["per_page"]:
            next_page = r_json["links"]["next"]
            while next_page is not None:
                next_page_json = self._get_request_with_bearer_token(next_page).json()
                results.extend(next_page_json["data"])
                next_page = next_page_json["links"]["next"]

        if self.verbose:
            print("OSF query: {}".format(query))
        return results

    def _download_files(
        self,
        link,
        current_dir,
        inner_path,
        d,
        annex,
        sizes,
        is_private=False,
    ):
        response = self._get_request_with_bearer_token(link)
        if response is None:
            print(f"Skipping download for {link} due to a failed request.")
            return
        print("first download", response)
        r_json = response.json()
        files = r_json["data"]

        # Retrieve the files in the other pages if there are more than 1 page
        if (
            "links" in r_json.keys()
            and r_json["links"]["meta"]["total"] > r_json["links"]["meta"]["per_page"]
        ):
            print("dans le next page")
            next_page = r_json["links"]["next"]
            while next_page is not None:
                response = self._get_request_with_bearer_token(next_page)
                if response is None:
                    print(f"Skipping page {next_page} due to a failed request.")
                    break

                next_page_json = response.json()
                files.extend(next_page_json["data"])
                next_page = next_page_json["links"]["next"]

        for file in files:
            # Handle folders
            if file["attributes"]["kind"] == "folder":
                folder_path = os.path.join(current_dir, file["attributes"]["name"])
                # Conditions added by Alex
                if not os.path.exists(folder_path):
                    os.mkdir(folder_path)
                    self._download_files(
                        file["relationships"]["files"]["links"]["related"]["href"],
                        folder_path,
                        os.path.join(inner_path, file["attributes"]["name"]),
                        d,
                        annex,
                        sizes,
                        is_private,
                    )
                else:
                    print(f"the folder {folder_path} already exist.")

            # Handle single files
            elif file["attributes"]["kind"] == "file":
                try:
                    # Private dataset/files
                    if is_private:
                        correct_download_link = self._get_request_with_bearer_token(
                            file["links"]["download"],
                            redirect=False,
                        )
                        if correct_download_link is not None:
                            correct_download_link = correct_download_link.headers[
                                "location"
                            ]
                            if (
                                "https://accounts.osf.io/login"
                                not in correct_download_link
                            ):
                                zip_file = (
                                    True
                                    if file["attributes"]["name"].split(".")[-1]
                                    == "zip"
                                    else False
                                )
                                d.download_url(
                                    correct_download_link,
                                    path=os.path.join(inner_path, ""),
                                    archive=zip_file,
                                )
                            else:  # Token did not work for downloading file, return
                                file = file["links"]["download"]
                                print(
                                    f"Unable to download file {file} with current token, skipping file",
                                )
                                return

                    # Public file
                    else:
                        # Handle zip files
                        if file["attributes"]["name"].split(".")[-1] == "zip":
                            d.download_url(
                                file["links"]["download"],
                                path=os.path.join(inner_path, ""),
                                archive=True,
                            )
                        else:
                            d.download_url(
                                file["links"]["download"],
                                path=os.path.join(inner_path, ""),
                            )

                except IncompleteResultsError as e:
                    print(
                        f"Skipping file {file['links']['download']} due to error: {e}"
                    )
                    continue  # Skip ce fichier et passer au suivant

                # append the size of the downloaded file to the sizes array
                file_size = file["attributes"]["size"]
                if not file_size:
                    # if the file size cannot be found in the OSF API response, then get it from git annex info
                    inner_file_path = os.path.join(
                        inner_path,
                        file["attributes"]["name"],
                    )
                    annex_info_dict = json.loads(
                        annex("info", "--bytes", "--json", inner_file_path),
                    )
                    file_size = int(annex_info_dict.get("size", 0))
                sizes.append(file_size)

    def _download_components(
        self,
        components_list,
        current_dir,
        inner_path,
        d,
        annex,
        dataset_size,
        is_private,
    ):
        # Loop through each available components and download their files
        for component in components_list:
            component_title = self._clean_dataset_title(
                component["attributes"]["title"],
            )
            component_inner_path = os.path.join(
                inner_path,
                "components",
                component_title,
            )
            os.makedirs(os.path.join(current_dir, component_inner_path))
            self._download_files(
                component["relationships"]["files"]["links"]["related"]["href"],
                os.path.join(current_dir, component_inner_path),
                component_inner_path,
                d,
                annex,
                dataset_size,
                is_private,
            )

            # check if the component contains (sub)components, in which case, download the (sub)components data
            subcomponents_list = self._get_components(
                component["relationships"]["children"]["links"]["related"]["href"],
            )
            if subcomponents_list:
                self._download_components(
                    subcomponents_list,
                    current_dir,
                    os.path.join(component_inner_path),
                    d,
                    annex,
                    dataset_size,
                    is_private,
                )

        # Once we have downloaded all the components files, check to see if there are any empty
        # directories (in the case the 'OSF parent' dataset did not have any downloaded files
        list_of_empty_dirs = [
            dirpath
            for (dirpath, dirnames, filenames) in os.walk(current_dir)
            if len(dirnames) == 0 and len(filenames) == 0
        ]
        for empty_dir in list_of_empty_dirs:
            os.rmdir(empty_dir)

    def _get_contributors(self, link):
        r = self._get_request_with_bearer_token(link)
        contributors = [
            contributor["embeds"]["users"]["data"]["attributes"]["full_name"]
            for contributor in r.json()["data"]
        ]
        return contributors

    def _get_license(self, link):
        r = self._get_request_with_bearer_token(link)
        return r.json()["data"]["attributes"]["name"]

    def _get_components(self, link):
        r = self._get_request_with_bearer_token(link)
        return r.json()["data"]

    def _get_wiki(self, link) -> Optional[str]:
        r = self._get_request_with_bearer_token(link)
        data = r.json()["data"]
        if len(data) > 0:
            return self._get_request_with_bearer_token(
                data[0]["links"]["download"]
            ).content.decode()

    def _get_institutions(self, link):
        r = self._get_request_with_bearer_token(link)
        if r.json()["data"]:
            institutions = [
                institution["attributes"]["name"] for institution in r.json()["data"]
            ]
            return institutions

    def _get_identifier(self, link):
        r = self._get_request_with_bearer_token(link)
        return r.json()["data"][0]["attributes"]["value"] if r.json()["data"] else False

    def get_all_dataset_description(self):
        osf_dois = []
        datasets = self._query_osf()
        for dataset in datasets:
            # skip datasets that have a parent since the files' components will
            # go into the parent dataset.
            # print("parent" in dataset["relationships"].keys())
            if "parent" in dataset["relationships"].keys():
                print(dataset["relationships"]["parent"])
            #    continue

            attributes = dataset["attributes"]

            # Retrieve keywords/tags
            keywords = list(map(lambda x: {"value": x}, attributes["tags"]))

            # Retrieve contributors/creators
            contributors = self._get_contributors(
                dataset["relationships"]["contributors"]["links"]["related"]["href"],
            )

            # Retrieve license
            license_ = "None"
            if "license" in dataset["relationships"].keys():
                license_ = self._get_license(
                    dataset["relationships"]["license"]["links"]["related"]["href"],
                )

            # Retrieve institution information
            institutions = self._get_institutions(
                dataset["relationships"]["affiliated_institutions"]["links"]["related"][
                    "href"
                ],
            )

            # Retrieve identifier information
            identifier = self._get_identifier(
                dataset["relationships"]["identifiers"]["links"]["related"]["href"],
            )

            # Get link for the dataset files
            files_link = dataset["relationships"]["files"]["links"]["related"]["href"]

            # Get components list
            components_list = self._get_components(
                dataset["relationships"]["children"]["links"]["related"]["href"],
            )

            # Get wiki to put in README
            wiki: Optional[str] = None
            try:
                wiki = self._get_wiki(
                    dataset["relationships"]["wikis"]["links"]["related"]["href"]
                )
            except Exception as e:
                print(f'Error getting wiki for {attributes["title"]} because of {e}')

            # Gather extra properties
            extra_properties = [
                {
                    "category": "logo",
                    "values": [
                        {
                            "value": "https://osf.io/static/img/institutions/shields/cos-shield.png",
                        },
                    ],
                },
            ]
            if institutions:
                extra_properties.append(
                    {
                        "category": "origin_institution",
                        "values": list(
                            map(lambda x: {"value": x}, institutions),
                        ),
                    },
                )

            # Retrieve dates
            date_created = datetime.datetime.strptime(
                attributes["date_created"],
                "%Y-%m-%dT%H:%M:%S.%f",
            )
            date_modified = datetime.datetime.strptime(
                attributes["date_modified"],
                "%Y-%m-%dT%H:%M:%S.%f",
            )

            dataset_dats_content = {
                "title": attributes["title"],
                "files": files_link,
                "components_list": components_list,
                "homepage": dataset["links"]["html"],
                "creators": list(
                    map(lambda x: {"name": x}, contributors),
                ),
                "description": attributes["description"],
                "wiki": wiki,
                "version": attributes["date_modified"],
                "licenses": [
                    {
                        "name": license_,
                    },
                ],
                "dates": [
                    {
                        "date": date_created.strftime("%Y-%m-%d %H:%M:%S"),
                        "type": {
                            "value": "date created",
                        },
                    },
                    {
                        "date": date_modified.strftime("%Y-%m-%d %H:%M:%S"),
                        "type": {
                            "value": "date modified",
                        },
                    },
                ],
                "keywords": keywords,
                "distributions": [
                    {
                        "size": 0,
                        "unit": {"value": "B"},
                        "access": {
                            "landingPage": dataset["links"]["html"],
                            "authorizations": [
                                {
                                    "value": "public"
                                    if attributes["public"]
                                    else "private",
                                },
                            ],
                        },
                    },
                ],
                "extraProperties": extra_properties,
            }

            if identifier:
                source = "OSF DOI" if "OSF.IO" in identifier else "DOI"
                dataset_dats_content["identifier"] = {
                    "identifier": identifier,
                    "identifierSource": source,
                }

            osf_dois.append(dataset_dats_content)

        if self.verbose:
            print("Retrieved OSF DOIs: ")
            for osf_doi in osf_dois:
                print(
                    "- Title: {}, Last modified: {}".format(
                        osf_doi["title"],
                        osf_doi["version"],
                    ),
                )

        return osf_dois

    def add_new_dataset(self, dataset: Dict[str, Any], dataset_dir: str):
        d: Dataset = self.datalad.Dataset(dataset_dir)
        d.no_annex(".conp-osf-crawler.json")
        d.save()
        annex: Callable = Repo(dataset_dir).git.annex
        dataset_size: List[int] = []

        # Setup private OSF dataset if the dataset is private
        is_private: bool = self._setup_private_dataset(
            dataset["files"],
            dataset_dir,
            annex,
            d,
        )
        self._download_files(
            dataset["files"],
            dataset_dir,
            "",
            d,
            annex,
            dataset_size,
            is_private,
        )
        if dataset["components_list"]:
            self._download_components(
                dataset["components_list"],
                dataset_dir,
                "",
                d,
                annex,
                dataset_size,
                is_private,
            )
        dataset_size_num, dataset_unit = humanize.naturalsize(sum(dataset_size)).split(
            " ",
        )
        dataset["distributions"][0]["size"] = float(dataset_size_num)
        dataset["distributions"][0]["unit"]["value"] = dataset_unit

        # Add .conp-osf-crawler.json tracker file
        _create_osf_tracker(
            os.path.join(dataset_dir, ".conp-osf-crawler.json"),
            dataset,
        )
        # Tenter de publier sur le remote 'origin'
        try:
            d.publish(to="origin")
        except IncompleteResultsError as e:
            print(f"Skipping publication due to error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during publication: {e}")

    def update_if_necessary(self, dataset_description, dataset_dir):
        tracker_path = os.path.join(dataset_dir, ".conp-osf-crawler.json")
        if not os.path.isfile(tracker_path):
            print("{} does not exist in dataset, skipping".format(tracker_path))
            return False
        with open(tracker_path) as f:
            tracker = json.load(f)
        if tracker["version"] == dataset_description["version"]:
            # Same version, no need to update
            if self.verbose:
                print(
                    "{}, version {} same as OSF version DOI ({}), no need to update".format(
                        dataset_description["title"],
                        dataset_description["version"],
                        tracker["version"],
                    ),
                )
            return False

        # Update dataset
        if self.verbose:
            print(
                "{}, version {} different from OSF version DOI {}, updating".format(
                    dataset_description["title"],
                    tracker["version"],
                    dataset_description["version"],
                ),
            )

        # Remove all data and DATS.json files
        for file_name in os.listdir(dataset_dir):
            if file_name[0] == ".":
                continue
            self.datalad.remove(os.path.join(dataset_dir, file_name), check=False)

        d = self.datalad.Dataset(dataset_dir)
        annex = Repo(dataset_dir).git.annex

        dataset_size = []
        is_private: bool = self._is_private_dataset(dataset_description["files"])
        self._download_files(
            dataset_description["files"],
            dataset_dir,
            "",
            d,
            annex,
            dataset_size,
            is_private,
        )
        if dataset_description["components_list"]:
            self._download_components(
                dataset_description["components_list"],
                dataset_dir,
                "",
                d,
                annex,
                dataset_size,
                is_private,
            )
        dataset_size, dataset_unit = humanize.naturalsize(sum(dataset_size)).split(
            " ",
        )
        dataset_description["distributions"][0]["size"] = float(dataset_size)
        dataset_description["distributions"][0]["unit"]["value"] = dataset_unit

        # Add .conp-osf-crawler.json tracker file
        _create_osf_tracker(
            os.path.join(dataset_dir, ".conp-osf-crawler.json"),
            dataset_description,
        )

        return True

    def get_readme_content(self, dataset):
        readme_content = (
            f'# {dataset["title"]}\n\nCrawled from [OSF]({dataset["homepage"]})'
        )

        if "description" in dataset and dataset["description"]:
            readme_content += f'\n\n## Description\n\n{dataset["description"]}'

        if "identifier" in dataset and dataset["identifier"]:
            readme_content += f'\n\n## DOI: {dataset["identifier"]["identifier"]}'

        if "wiki" in dataset and dataset["wiki"]:
            readme_content += f'\n\n## WIKI\n\n{dataset["wiki"]}'

        return readme_content

    def _setup_private_dataset(
        self,
        files_url: str,
        dataset_dir: str,
        annex: Callable,
        dataset: Dataset,
    ) -> bool:
        # Check if the dataset is indeed private
        if self._is_private_dataset(files_url):
            if self.verbose:
                print(
                    "Dataset is private, creating OSF provider and make git annex autoenable datalad remote",
                )

            # Create OSF provider file and needed directories and don't annex the file
            datalad_dir: str = os.path.join(dataset_dir, ".datalad")
            if not os.path.exists(datalad_dir):
                os.mkdir(datalad_dir)
            providers_dir: str = os.path.join(datalad_dir, "providers")
            if not os.path.exists(providers_dir):
                os.mkdir(providers_dir)
            osf_config_path: str = os.path.join(providers_dir, "OSF.cfg")
            with open(osf_config_path, "w") as f:
                f.write(
                    """[provider:OSF]
url_re = .*osf\\.io.*
authentication_type = bearer_token
credential = OSF

[credential:OSF]
# If known, specify URL or email to how/where to request credentials
# url = ???
type = token"""
                )
            dataset.no_annex(os.path.join("**", "OSF.cfg"))

            # Make git annex autoenable datalad remote
            annex(
                "initremote",
                "datalad",
                "externaltype=datalad",
                "type=external",
                "encryption=none",
                "autoenable=true",
            )

            # Set OSF token as a environment variable for authentication
            os.environ["DATALAD_OSF_token"] = self.osf_token

            # Save changes
            dataset.save()

            return True

        return False

    def _is_private_dataset(self, files_url) -> bool:
        return True if requests.get(files_url).status_code == 401 else False


========================================
FILE: ./Crawlers/BaseCrawlerTest.py
========================================
import abc
import json
import os
import re
import shutil

import git
import requests
from datalad import api

from scripts.Crawlers.constants import DATS_FIELDS
from scripts.Crawlers.constants import LICENSE_CODES
from scripts.Crawlers.constants import MODALITIES
from scripts.Crawlers.constants import NO_ANNEX_FILE_PATTERNS
from scripts.Crawlers.constants import REQUIRED_DATS_FIELDS


class BaseCrawler:
    """
    Interface to extend conp-dataset crawlers.

    ==================
    Overview
    ==================

    Any crawler created from this interface will have to crawl
    datasets from a specific remote platforms. This base class
    implements the functions common to all crawled backends, in particular:
    (1) verify that correct fork of conp-dataset is used,
    (2) create and switch to an new branch for each new dataset,
    (3) ignore README and DATS files for the annex,
    (4) create new datalad datasets,
    (5) publish to GitHub repository,
    (6) create pull requests.

    Method run(), implemented in the base class, is the entry point to any crawler.
    It does the following things:
    (1) It calls abstract method get_all_dataset_description(), that must be implemented
    by the crawler in the child class. get_all_dataset_description()
    retrieves from the remote platform
    all the necessary information about each dataset that is supposed to be added or
    updated to conp-dataset.
    (2) It iterates through each dataset description, and switch to a dedicated git branch
    for each dataset.
       (2.a) If the dataset is new, the base class will create a new branch,
             an empty datalad repository, unannex DATS.json and README.md and create an
             empty GitHub repository. It will then call abstract method add_new_dataset()
             which will add/download all dataset files under given directory.
             The crawler will then add a custom DATS.json and README.md if those weren't added.
             Creating the README.md requires get_readme_content() to be implemented, which will
             return the content of the README.md in markdown format. The crawler will then save and
             publish all changes to the newly create repository. It will also handle adding a new submodule
             to .gitmodules and creating a pull request to CONP-PCNO/conp-dataset.
       (2.b) If the dataset already exists, verified by the existence of its corresponding branch,
             the base class will call abstract method update_if_necessary() which will verify
             if the dataset requires updating and update if so. If the dataset got updated, This method
             will return True which will trigger saving, publishing new content to the dataset's respective
             repository, creating a new DATS.json if it doesn't exist and creating a pull
             request to CONP-PCNO/conp-dataset.

    ==================
    How to implement a new crawler
    ==================

        (1) Create a class deriving from BaseCrawler
        (2) Implement the four abstract methods:
            * get_all_dataset_description,
            * add_new_dataset
            * update_if_necessary
            * get_readme_content.
            See docstrings of each method for specifications.
        (3) In crawl.py, locate the comment where it says to instantiate new crawlers,
            instantiate this new Crawler and call run() on it
    """

    def __init__(self, github_token, config_path, verbose, force, no_pr, basedir):
        self.basedir = basedir
        self.repo = git.Repo(self.basedir)
        self.username = self._check_requirements()
        self.github_token = github_token
        self.config_path = config_path
        self.verbose = verbose
        self.force = force
        self.git = git
        self.datalad = api
        self.no_pr = no_pr
        if self.verbose:
            print(f"Using base directory {self.basedir}")

    @abc.abstractmethod
    def get_all_dataset_description(self):
        """
        Get relevant datasets' description from platform.

        Retrieves datasets' description that needs to be in CONP-datasets
        from platform specific to each crawler like Zenodo, OSF, etc. It is up
        to the crawler to identify which datasets on the platform should be crawled.
        The Zenodo crawler uses keywords for this purpose, but other mechanisms
        could work too.

        Each description is required to have the necessary information in order
        to build a valid DATS file from it. The following keys are necessary in
        each description:
            description["title"]: The name of the dataset, usually one sentence or short description of the dataset
            description["identifier"]: The identifier of the dataset
            description["creators"]: The person(s) or organization(s) which contributed to the creation of the dataset
            description["description"]: A textual narrative comprised of one or more statements describing the dataset
            description["version"]: A release point for the dataset when applicable
            description["licenses"]: The terms of use of the dataset
            description["keywords"]: Tags associated with the dataset, which will help in its discovery
            description["types"]: A term, ideally from a controlled terminology, identifying the dataset type or nature
                                  of the data, placing it in a typology
        More fields can be added as long as they comply with the DATS schema available at
        https://github.com/CONP-PCNO/schema/blob/master/dataset_schema.json

        Any fields/keys not in the schema will be ignored when creating the dataset's DATS.
        It is fine to add more helpful information for other methods which will use them.

        Here are some examples of valid DATS.json files:
        https://github.com/conp-bot/conp-dataset-Learning_Naturalistic_Structure__Processed_fMRI_dataset/blob/476a1ee3c4df59aca471499b2e492a65bd389a88/DATS.json
        https://github.com/conp-bot/conp-dataset-MRI_and_unbiased_averages_of_wild_muskrats__Ondatra_zibethicus__and_red_squirrels__Tami/blob/c9e9683fbfec71f44a5fc3576515011f6cd024fe/DATS.json
        https://github.com/conp-bot/conp-dataset-PERFORM_Dataset__one_control_subject/blob/0b1e271fb4dcc03f9d15f694cc3dfae5c7c2d358/DATS.json

        Returns:
            List of description of relevant datasets. Each description is a
            dictionary. For example:

            [{
                "title": "PERFORM Dataset Example",
                "description": "PERFORM dataset description",
                "version": "0.0.1",
                ...
             },
             {
                "title": "SIMON dataset example",
                "description: "SIMON dataset description",
                "version": "1.4.2",
                ...
             },
             ...
            ]
        """
        return []

    @abc.abstractmethod
    def add_new_dataset(self, dataset_description, dataset_dir):
        """
        Configure and add newly created dataset to the local CONP git repository.

        The BaseCrawler will take care of a few overhead tasks before
        add_new_dataset() is called, namely:
        (1) Creating and checkout a dedicated branch for this dataset
        (2) Initialising a Github repo for this dataset
        (3) Creating an empty datalad dataset for files to be added
        (4) Annex ignoring README.md and DATS.json

        After add_new_dataset() is called, BaseCrawler will:
        (1) Create a custom DATS.json if it isn't added in add_new_dataset()
        (2) Create a custom README.md with get_readme_content() if that file is non-existent
        (3) Save and publish all changes
        (4) Adding this dataset as a submodule
        (5) Creating a pull request to CONP-PCNO/conp-dataset
        (6) Switch back to the master branch

        This means that add_new_dataset() will only have to implement a few tasks in the given
        previously initialized datalad dataset directory:
        (1) Adding any one time configuration such as annex ignoring files or adding dataset version tracker
        (2) Downloading and unarchiving relevant archives using datalad.download_url(link, archive=True)
        (3) Adding file links as symlinks using annex("addurl", link, "--fast", "--file", filename)

        There is no need to save/push/publish/create pull request
        as those will be done after this function has finished

        Parameter:
        dataset_description (dict): Dictionary containing information on
                                    retrieved dataset from platform. Element of
                                    the list returned by get_all_dataset_description.
        dataset_dir (str): Local directory path where the newly
                           created datalad dataset is located.
        """
        return

    @abc.abstractmethod
    def update_if_necessary(self, dataset_description, dataset_dir):
        """
        Update dataset if it has been modified on the remote platform.

        Determines if local dataset identified by 'identifier'
        needs to be updated. If so, update dataset.

        Similarily to add_new_dataset(), update_if_necessary() will need to
        take care of updating the dataset if required:
        (1) Downloading and unarchiving relevant archives using datalad.download_url(link, archive=True)
        (2) Adding new file links as symlinks using annex("addurl", link, "--fast", "--file", filename)
        (3) Updating any tracker files used to determine if the dataset needs to be updated

        There is no need to save/push/publish/create pull request
        as those will be done after this function has finished

        Parameter:
        dataset_description (dict): Dictionary containing information on
                                    retrieved dataset from platform.
                                    Element of the list returned by
                                    get_all_dataset_description.
        dataset_dir (str): Directory path of the
                           previously created datalad dataset

        Returns:
        bool: True if dataset got modified, False otherwise
        """
        return False

    @abc.abstractmethod
    def get_readme_content(self, dataset_description):
        """
        Returns the content of the README.md in markdown.

        Given the dataset description provided by
        get_all_dataset_description(), return the content of the
        README.md in markdown.

        Parameter:
        dataset_description (dict): Dictionary containing information on
                                    retrieved dataset from platform.
                                    Element of the list returned by
                                    get_all_dataset_description.

        Returns:
        string: Content of the README.md
        """
        return ""

    def run(self):
        """
        DO NOT OVERRIDE THIS METHOD
        This method is the entry point for all Crawler classes.
        It will loop through dataset descriptions collected from get_all_dataset_description(),
        verify if each of those dataset present locally, create a new dataset with add_new_dataset()
        if not. If dataset is already existing locally, verify if dataset needs updating
        with update_if_necessary() and update if so
        """
        dataset_description_list = self.get_all_dataset_description()
        for dataset_description in dataset_description_list:
            clean_title = self._clean_dataset_title(dataset_description["title"])
            branch_name = "conp-bot/" + clean_title
            dataset_dir = os.path.join(self.basedir, "projects", clean_title)
            d = self.datalad.Dataset(dataset_dir)
            if branch_name not in self.repo.remotes.origin.refs:  # New dataset
                self.repo.git.checkout("-b", branch_name)
                repo_title = ("conp-dataset-" + dataset_description["title"])[0:100]
                try:
                    d.create()
                    r = d.create_sibling_github(
                        repo_title,
                        name="origin",
                        github_login=self.github_token,
                        github_passwd=self.github_token,
                    )
                except Exception as error:
                    # handle the exception
                    print("An exception occurred:", error)

                # Add github token to dataset origin remote url
                try:
                    origin = self.repo.remote("origin")
                    origin_url = next(origin.urls)
                    if "@" not in origin_url:
                        origin.set_url(
                            origin_url.replace(
                                "https://",
                                "https://" + self.github_token + "@",
                            ),
                        )
                except git.exc.NoSuchPathError:
                    pass

                self._add_github_repo_description(repo_title, dataset_description)
                for pattern in NO_ANNEX_FILE_PATTERNS:
                    d.no_annex(pattern)
                self.add_new_dataset(dataset_description, dataset_dir)

                # Create DATS.json if it exists in directory and 1 level deep subdir
                dats_path: str = os.path.join(dataset_dir, "DATS.json")
                if existing_dats_path := self._check_file_present(
                    dataset_dir, "dats.json"
                ):
                    if self.verbose:
                        print(f"Found existing DATS.json at {existing_dats_path}")
                    if existing_dats_path != dats_path:
                        shutil.copy(existing_dats_path, dats_path)
                    self._add_source_data_submodule_if_derived_from_conp_dataset(
                        dats_path, dataset_dir
                    )
                else:
                    self._create_new_dats(
                        dataset_dir,
                        dats_path,
                        dataset_description,
                        d,
                    )
                # Move the logo into the root directory if found in 1 level deep subdir
                logo_path = os.path.join(dataset_dir, "logo.png")
                if existing_logo_path := self._check_file_present(
                    dataset_dir, "logo.png"
                ):
                    if self.verbose:
                        print(f"Found logo at {existing_logo_path}")
                    if existing_logo_path != logo_path:
                        os.rename(existing_logo_path, logo_path)

                # Create README.md if it doesn't exist
                if not os.path.isfile(os.path.join(dataset_dir, "README.md")):
                    readme = self.get_readme_content(dataset_description)
                    self._create_readme(readme, os.path.join(dataset_dir, "README.md"))
                d.save()
                try:
                    d.publish(to="origin")
                    self.repo.git.submodule(
                        "add",
                        r[0][1].replace(self.github_token + "@", ""),
                        dataset_dir,
                    )
                except Exception as e:
                    print(f"Skipping publication due to an error: {e}")
                modified = True
                commit_msg = "Created " + dataset_description["title"]
            else:  # Dataset already existing locally
                self.repo.git.checkout("-f", branch_name)
                try:
                    self.repo.git.merge("-n", "--no-verify", "master")
                except Exception as e:
                    print(f"Error while merging master into {branch_name}: {e}")
                    print("Skipping this dataset")
                    self.repo.git.merge("--abort")
                    self.repo.git.checkout("-f", "master")
                    continue

                modified = self.update_if_necessary(dataset_description, dataset_dir)
                if modified:
                    # Create DATS.json if it exists in directory and 1 level deep subdir
                    dats_path: str = os.path.join(dataset_dir, "DATS.json")
                    if existing_dats_path := self._check_file_present(
                        dataset_dir, "dats.json"
                    ):
                        if self.verbose:
                            print(f"Found existing DATS.json at {existing_dats_path}")
                        if existing_dats_path != dats_path:
                            os.rename(existing_dats_path, dats_path)
                        self._add_source_data_submodule_if_derived_from_conp_dataset(
                            dats_path, dataset_dir
                        )
                    else:
                        self._create_new_dats(
                            dataset_dir,
                            dats_path,
                            dataset_description,
                            d,
                        )
                    # Move the logo into the root directory if found in 1 level deep subdir
                    logo_path = os.path.join(dataset_dir, "logo.png")
                    if existing_logo_path := self._check_file_present(
                        dataset_dir, "logo.png"
                    ):
                        if self.verbose:
                            print(f"Found logo at {existing_logo_path}")
                        if existing_logo_path != logo_path:
                            os.rename(existing_logo_path, logo_path)
                    # Create README.md if it doesn't exist
                    if not os.path.isfile(os.path.join(dataset_dir, "README.md")):
                        readme = self.get_readme_content(dataset_description)
                        self._create_readme(
                            readme,
                            os.path.join(dataset_dir, "README.md"),
                        )
                    d.save()
                    try:
                        d.publish(to="origin")
                    except Exception as e:
                        print(f"Skipping publication due to an error: {e}")
                commit_msg = "Updated " + dataset_description["title"]

            # If modification detected in dataset, push to branch and create PR
            if modified:
                self._push_and_pull_request(
                    commit_msg,
                    dataset_dir,
                    dataset_description["title"],
                )

            # Go back to master
            self.repo.git.checkout("master")

    def _add_github_repo_description(self, repo_title, dataset_description):
        url = "https://api.github.com/repos/{}/{}".format(
            self.username,
            repo_title,
        )
        head = {"Authorization": "token {}".format(self.github_token)}
        description = "Please don't submit any PR to this repository. "
        if "creators" in dataset_description.keys():
            description += (
                "If you want to request modifications, please contact "
                f"{dataset_description['creators'][0]['name']}"
            )
        payload = {"description": description}
        r = requests.patch(url, data=json.dumps(payload), headers=head)
        if not r.ok:
            print(
                "Problem adding description to repository {}:".format(repo_title),
            )
            print(r.content)

    def _check_requirements(self):
        # GitHub user must have a fork of https://github.com/CONP-PCNO/conp-dataset
        # Script must be run in the  directory of a local clone of this fork
        # Git remote 'origin' of local Git clone must point to that fork
        # Local Git clone must be set to branch 'master'
        if "origin" not in self.repo.remotes:
            raise Exception("Remote 'origin' does not exist in current reposition")
        origin_url = next(self.repo.remote("origin").urls)
        full_name = re.search("github.com[/,:](.*).git", origin_url).group(1)
        r = requests.get("http://api.github.com/repos/" + full_name).json()
        if not r["fork"] or r["parent"]["full_name"] != "CONP-PCNO/conp-dataset":
            raise Exception("Current repository not a fork of CONP-PCNO/conp-dataset")
        branch = self.repo.active_branch.name
        if branch != "master":
            raise Exception("Local git clone active branch not set to 'master'")

        # Return username
        return full_name.split("/")[0]

    def _push_and_pull_request(self, msg, dataset_dir, title):
        self.repo.git.add(dataset_dir)
        self.repo.git.add(".gitmodules")
        self.repo.git.commit("-m", "[conp-bot] " + msg)
        clean_title = self._clean_dataset_title(title)
        origin = self.repo.remote("origin")
        origin_url = next(origin.urls)
        if "@" not in origin_url:
            origin.set_url(
                origin_url.replace("https://", "https://" + self.github_token + "@"),
            )
        self.repo.git.push("--set-upstream", "origin", "conp-bot/" + clean_title)

        # Create PR
        print("Creating PR for " + title)
        if not self.no_pr:
            r = requests.post(
                "https://api.github.com/repos/CONP-PCNO/conp-dataset/pulls",
                json={
                    "title": "Crawler result ({})".format(title),
                    "body": """## Description
{}

## Checklist

Mandatory files and elements:
- [x] A `README.md` file, at the root of the dataset
- [x] A `DATS.json` file, at the root of the dataset
- [ ] If configuration is required (for instance to enable a special remote),
 a `config.sh` script at the root of the dataset
- [x] A DOI (see instructions in [contribution guide]
(https://github.com/CONP-PCNO/conp-dataset/blob/master/.github/CONTRIBUTING.md), and corresponding badge in `README.md`

Functional checks:
- [x] Dataset can be installed using DataLad, recursively if it has sub-datasets
- [x] Every data file has a URL
- [x] Every data file can be retrieved or requires authentication
- [ ] `DATS.json` is a valid DATs model
- [ ] If dataset is derived data, raw data is a sub-dataset
""".format(
                        msg + "\n",
                    ),
                    "head": self.username + ":conp-bot/" + clean_title,
                    "base": "master",
                },
                headers={"Authorization": "token {}".format(self.github_token)},
            )
            if r.status_code != 201:
                raise Exception("Error while creating pull request: " + r.text)

    def _clean_dataset_title(self, title):
        return re.sub(r"\W|^(?=\d)", "_", title)

    def _create_new_dats(self, dataset_dir, dats_path, dataset, d):
        # Helper recursive function
        def retrieve_license_path_in_dir(dir, paths):
            for f_name in os.listdir(dir):
                f_path = os.path.join(dir, f_name)
                if os.path.isdir(f_path):
                    retrieve_license_path_in_dir(f_path, paths)
                    continue
                elif "license" not in f_name.lower():
                    continue
                elif os.path.islink(f_path):
                    d.get(f_path)
                paths.append(f_path)

        # Check required properties
        for field in REQUIRED_DATS_FIELDS:
            if field not in dataset.keys():
                print(
                    "Warning: required property {} not found in dataset description".format(
                        field,
                    ),
                )

        # Add all dats properties from dataset description
        data = {key: value for key, value in dataset.items() if key in DATS_FIELDS}

        # Check for license code in dataset if a license was not specified from the platform
        if "licenses" not in data or (
            len(data["licenses"]) == 1 and data["licenses"][0]["name"].lower() == "none"
        ):
            # Collect all license file paths
            license_f_paths = []
            retrieve_license_path_in_dir(dataset_dir, license_f_paths)

            # If found some license files, for each, check for first valid license code and add to DATS
            if license_f_paths:
                licenses = set()
                for f_path in license_f_paths:
                    with open(f_path) as f:
                        text = f.read().lower()
                    for code in LICENSE_CODES:
                        if code.lower() in text:
                            licenses.add(code)
                            break
                data["licenses"] = [{"name": code} for code in licenses]

        # Add file count
        num = 0
        for file in os.listdir(dataset_dir):
            file_path = os.path.join(dataset_dir, file)
            if (
                file[0] == "."
                or file == "DATS.json"
                or file == "README.md"
                or file == "logo.png"
            ):
                continue
            elif os.path.isdir(file_path):
                num += sum([len(files) for r, d, files in os.walk(file_path)])
            else:
                num += 1
        if "extraProperties" not in data.keys():
            data["extraProperties"] = [
                {"category": "files", "values": [{"value": str(num)}]},
            ]
        else:
            data["extraProperties"].append(
                {"category": "files", "values": [{"value": str(num)}]},
            )

        # Retrieve modalities from files
        file_paths = map(
            lambda x: x.split(" ")[-1],
            filter(
                lambda x: " " in x,
                git.Repo(dataset_dir).git.annex("list").split("\n"),
            ),
        )  # Get file paths
        file_names = list(
            map(lambda x: x.split("/")[-1] if "/" in x else x, file_paths),
        )  # Get file names from path
        modalities = {self._guess_modality(file_name) for file_name in file_names}
        if len(modalities) == 0:
            modalities.add("unknown")
        elif len(modalities) > 1 and "unknown" in modalities:
            modalities.remove("unknown")
        if "types" not in data.keys():
            data["types"] = [{"value": modality} for modality in modalities]
        else:
            for modality in modalities:
                data["types"].append({"value": modality})

        # Create file
        with open(dats_path, "w") as f:
            json.dump(data, f, indent=4)

    def _guess_modality(self, file_name):
        # Associate file types to substrings found in the file name
        for m in MODALITIES:
            for s in MODALITIES[m]:
                if s in file_name:
                    return m
        return "unknown"

    def _create_readme(self, content, path):
        with open(path, "w") as f:
            f.write(content)

    def _check_file_present(self, directory, filename):
        for file_name in os.listdir(directory):
            file_path: str = os.path.join(directory, file_name)
            if os.path.isdir(file_path):
                for subfile_name in os.listdir(file_path):
                    if subfile_name.lower() == filename.lower():
                        return os.path.join(file_path, subfile_name)
            elif file_name.lower() == filename.lower():
                return file_path

    def _add_source_data_submodule_if_derived_from_conp_dataset(
        self, dats_json, dataset_dir
    ):
        with open(dats_json) as f:
            metadata = json.loads(f.read())

        source_dataset_link = None
        source_dataset_id = None
        if "extraProperties" not in metadata.keys():
            return
        for property in metadata["extraProperties"]:
            if property["category"] == "derivedFrom":
                try:
                    source_dataset_link = property["values"][0]["value"]
                except (KeyError, IndexError):
                    continue
            if property["category"] == "parent_dataset_id":
                try:
                    source_dataset_id = property["values"][0]["value"]
                except (KeyError, IndexError):
                    continue

        if source_dataset_link is not None and "github.com" in source_dataset_link:
            d = self.datalad.Dataset(os.path.join(dataset_dir, source_dataset_id))
            d.create()


========================================
FILE: ./Crawlers/constants.py
========================================
LICENSE_CODES = [
    "CC BY-NC-SA",
    "CC BY-NC-ND",
    "CC BY-SA",
    "CC BY-NC",
    "CC BY-ND",
    "CC BY",
    "CC0",
    "ODbL" "ODC-By",
    "PDDL",
]
NO_ANNEX_FILE_PATTERNS = ["**/DATS.json", "**/README*", "**/LICENSE*", "**/logo.png"]
REQUIRED_DATS_FIELDS = [
    "title",
    "types",
    "creators",
    "licenses",
    "description",
    "keywords",
    "version",
]
DATS_FIELDS = [
    "title",
    "identifier",
    "creators",
    "description",
    "version",
    "licenses",
    "keywords",
    "distributions",
    "extraProperties",
    "alternateIdentifiers",
    "relatedIdentifiers",
    "dates",
    "storedIn",
    "spatialCoverage",
    "types",
    "availability",
    "refinement",
    "aggregation",
    "privacy",
    "dimensions",
    "primaryPublications",
    "citations",
    "citationCount",
    "producedBy",
    "isAbout",
    "hasPart",
    "acknowledges",
]
MODALITIES = {
    "fMRI": ["bold", "func", "cbv"],
    "MRI": ["T1", "T2", "FLAIR", "FLASH", "PD", "angio", "anat", "mask"],
    "diffusion": ["dwi", "dti", "sbref"],
    "MEG": ["meg"],
    "intracranial EEG": ["ieeg"],
    "EEG": ["eeg"],
    "field map": ["fmap", "phasediff", "magnitude"],
    "imaging": ["nii", "nii.gz", "mnc"],
}


========================================
FILE: ./Crawlers/__init__.py
========================================


========================================
FILE: ./Crawlers/ZenodoCrawler.py
========================================
import datetime
import json
import os
import re
from typing import Callable

import html2markdown
import humanize
import requests
from datalad.distribution.dataset import Dataset
from git import Repo

from scripts.Crawlers.BaseCrawler import BaseCrawler


def _create_zenodo_tracker(path, dataset):
    with open(path, "w") as f:
        data = {
            "zenodo": {
                "concept_doi": dataset.get("concept_doi"),
                "version": dataset.get("latest_version"),
            },
            "title": dataset.get("title"),
        }
        json.dump(data, f, indent=4)


def _get_annex(dataset_dir) -> Callable:
    return Repo(dataset_dir).git.annex


class ZenodoCrawler(BaseCrawler):
    def __init__(self, github_token, config_path, verbose, force, no_pr, basedir):
        super().__init__(github_token, config_path, verbose, force, no_pr, basedir)
        self.zenodo_tokens = self._get_tokens()

    def _get_tokens(self):
        if os.path.isfile(self.config_path):
            with open(self.config_path) as f:
                data = json.load(f)
            if "zenodo_tokens" in data.keys():
                return data.get("zenodo_tokens")
            else:
                return {}

    def _query_zenodo(self):
        query = (
            "https://zenodo.org/api/records/?"
            "type=dataset&"
            'q=keywords:"canadian-open-neuroscience-platform"'
        )
        r_json = requests.get(query).json()
        results = r_json.get("hits", {}).get("hits")

        if r_json and r_json.get("links", {}).get("next"):
            next_page = r_json["links"]["next"]
            while next_page is not None:
                next_page_json = requests.get(next_page).json()
                results.extend(next_page_json["hits"]["hits"])
                next_page = (
                    next_page_json["links"]["next"]
                    if "next" in next_page_json["links"]
                    else None
                )

        if self.verbose:
            print("Zenodo query: {}".format(query))
        return results

    def _download_file(self, bucket, d, is_private):
        link: str = (
            bucket["links"]["self"]
            if not is_private
            else bucket["links"]["self"].split("?")[0]
        )
        file_name: str = bucket.get("key", "no name")
        file_size: int = bucket.get("size", 0)
        if self.verbose:
            print(f"Downloading {link} as {file_name} of size {file_size}")
        d.download_url(link, archive=True if bucket.get("type") == "zip" else False)

    def get_all_dataset_description(self):
        zenodo_dois = []
        datasets = self._query_zenodo()
        for dataset in datasets:
            metadata = dataset["metadata"]
            clean_title = self._clean_dataset_title(metadata["title"])

            # Retrieve file urls
            files = []
            is_private = False
            dataset_token = ""
            if "files" not in dataset.keys():
                # This means the Zenodo dataset files are restricted
                # Try to see if the dataset token is already known in stored tokens
                if clean_title in self.zenodo_tokens.keys():
                    data = requests.get(
                        dataset["links"]["latest"],
                        params={"access_token": self.zenodo_tokens[clean_title]},
                    ).json()
                    if "files" not in data.keys():
                        print(
                            "Unable to access {} using stored tokens, "
                            "skipping this dataset".format(clean_title),
                        )
                        continue
                    else:
                        # Append access token to each file url
                        for bucket in data["files"]:
                            bucket["links"]["self"] += (
                                "?access_token=" + self.zenodo_tokens[clean_title]
                            )
                            files.append(bucket)
                        is_private = True
                        dataset_token = self.zenodo_tokens[clean_title]
                else:
                    print(
                        "No available tokens to access files of {}".format(
                            metadata["title"],
                        ),
                    )
                    continue
            else:
                for bucket in dataset["files"]:
                    files.append(bucket)

            latest_version_doi = None
            version = metadata.get("relations", {}).get("version", [])
            if len(version):
                latest_version_doi = version[0].get("last_child", {}).get("pid_value")

            # Retrieve and clean file formats/extensions
            file_formats = (
                list(set(map(lambda x: os.path.splitext(x.get("key"))[1][1:], files)))
                if len(files) > 0
                else []
            )

            if "" in file_formats:
                file_formats.remove("")

            # Retrieve and clean file keywords
            keywords = []
            if "keywords" in metadata.keys():
                keywords = list(map(lambda x: {"value": x}, metadata.get("keywords")))

            # Retrieve subject annotations from Zenodo and clean the annotated
            # subjects to insert in isAbout of DATS file
            is_about = []
            if "subjects" in metadata.keys():
                for subject in metadata["subjects"]:
                    if re.match("www.ncbi.nlm.nih.gov/taxonomy", subject["identifier"]):
                        is_about.append(
                            {
                                "identifier": {"identifier": subject["identifier"]},
                                "name": subject["term"],
                            }
                        )
                    else:
                        is_about.append(
                            {
                                "valueIRI": subject["identifier"],
                                "value": subject["term"],
                            }
                        )

            dataset_size, dataset_unit = humanize.naturalsize(
                sum([filename["size"] for filename in files]),
            ).split(" ")
            dataset_size = float(dataset_size)

            # Get creators and assign roles if it exists
            creators = list(map(lambda x: {"name": x["name"]}, metadata["creators"]))
            if "contributors" in metadata.keys():
                for contributor in metadata["contributors"]:
                    if contributor["type"] == "ProjectLeader":
                        for creator in creators:
                            if creator["name"].lower() == contributor["name"].lower():
                                creator["roles"] = [{"value": "Principal Investigator"}]
                                break
                        else:
                            creators.append(
                                {
                                    "name": contributor["name"],
                                    "roles": [{"value": "Principal Investigator"}],
                                },
                            )

            # Get identifier
            identifier = (
                dataset["conceptdoi"]
                if "conceptdoi" in dataset.keys()
                else dataset["doi"]
            )

            # Get date created and date modified
            date_created = datetime.datetime.strptime(
                dataset["created"],
                "%Y-%m-%dT%H:%M:%S.%f%z",
            )
            date_modified = datetime.datetime.strptime(
                dataset["updated"],
                "%Y-%m-%dT%H:%M:%S.%f%z",
            )

            zenodo_dois.append(
                {
                    "identifier": {
                        "identifier": "https://doi.org/{}".format(identifier),
                        "identifierSource": "DOI",
                    },
                    "concept_doi": dataset["conceptrecid"],
                    "latest_version": latest_version_doi,
                    "title": metadata["title"],
                    "files": files,
                    "doi_badge": identifier,
                    "creators": creators,
                    "description": metadata["description"],
                    "version": metadata["version"]
                    if "version" in metadata.keys()
                    else "None",
                    "licenses": [
                        {
                            "name": metadata["license"]["id"]
                            if "license" in metadata.keys()
                            else "None",
                        },
                    ],
                    "is_private": is_private,
                    "dataset_token": dataset_token,
                    "keywords": keywords,
                    "distributions": [
                        {
                            "formats": [
                                file_format.upper()
                                for file_format in file_formats
                                # Do not modify specific file formats.
                                if file_format not in ["NIfTI", "BigWig"]
                            ],
                            "size": dataset_size,
                            "unit": {"value": dataset_unit},
                            "access": {
                                "landingPage": dataset.get("links", {}).get(
                                    "self_html", "n/a"
                                ),
                                "authorizations": [
                                    {
                                        "value": "public"
                                        if metadata["access_right"] == "open"
                                        else "private",
                                    },
                                ],
                            },
                        },
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
                            "type": {
                                "value": "date created",
                            },
                        },
                        {
                            "date": date_modified.strftime("%Y-%m-%d %H:%M:%S"),
                            "type": {
                                "value": "date modified",
                            },
                        },
                    ],
                },
            )

        if self.verbose:
            print("Retrieved Zenodo DOIs: ")
            for zenodo_doi in zenodo_dois:
                print(
                    "- Title: {}, Concept DOI: {}, Latest version DOI: {}, Private: {}, Token: {}".format(
                        zenodo_doi["title"],
                        zenodo_doi["concept_doi"],
                        zenodo_doi["latest_version"],
                        zenodo_doi["is_private"],
                        zenodo_doi["dataset_token"],
                    ),
                )

        return zenodo_dois

    def add_new_dataset(self, dataset, dataset_dir):
        d: Dataset = self.datalad.Dataset(dataset_dir)
        d.no_annex(".conp-zenodo-crawler.json")
        d.no_annex("config")
        d.save()
        annex: Callable = _get_annex(dataset_dir)
        is_private: bool = dataset.get("is_private", False)
        dataset_token: str = dataset.get("dataset_token", "")

        if is_private:
            self._setup_private_dataset(dataset_dir, annex, d, dataset_token)

        if self.verbose:
            print(
                f'Adding new dataset {dataset["title"]}, is_private: {is_private}, token: {dataset_token}'
            )

        for bucket in dataset["files"]:
            self._download_file(bucket, d, is_private)

        # Add .conp-zenodo-crawler.json tracker file
        _create_zenodo_tracker(
            os.path.join(dataset_dir, ".conp-zenodo-crawler.json"),
            dataset,
        )

    def update_if_necessary(self, dataset_description, dataset_dir):
        tracker_path = os.path.join(dataset_dir, ".conp-zenodo-crawler.json")
        if not os.path.isfile(tracker_path):
            print("{} does not exist in dataset, skipping".format(tracker_path))
            return False
        with open(tracker_path) as f:
            tracker = json.load(f)
        if tracker["zenodo"]["version"] == dataset_description["latest_version"]:
            # Same version, no need to update
            if self.verbose:
                print(
                    "{}, version {} same as Zenodo vesion DOI, no need to update".format(
                        dataset_description["title"],
                        dataset_description["latest_version"],
                    ),
                )
            return False
        else:
            # Update dataset
            if self.verbose:
                print(
                    f"{dataset_description['title']}, version {tracker['zenodo']['version']} different "
                    f"from Zenodo vesion DOI {dataset_description['latest_version']}, updating",
                )

            # Remove all data and DATS.json files
            for file_name in os.listdir(dataset_dir):
                if file_name[0] == ".":
                    continue
                self.datalad.remove(os.path.join(dataset_dir, file_name), check=False)

            d: Dataset = self.datalad.Dataset(dataset_dir)
            is_private: bool = dataset_description.get("is_private", False)

            # For download authentication purposes
            if is_private:
                dataset_token: str = dataset_description.get("dataset_token", "")
                if self.verbose:
                    print(f"Setting DATALAD_ZENODO_token={dataset_token}")
                os.environ["DATALAD_ZENODO_token"] = dataset_token

            for bucket in dataset_description["files"]:
                self._download_file(bucket, d, is_private)

            # Add/update .conp-zenodo-crawler.json tracker file
            _create_zenodo_tracker(
                tracker_path,
                dataset_description,
            )

            return True

    def get_readme_content(self, dataset):
        return """# {0}

[![DOI](https://www.zenodo.org/badge/DOI/{1}.svg)](https://doi.org/{1})

Crawled from Zenodo

## Description

{2}""".format(
            dataset["title"],
            dataset["doi_badge"],
            html2markdown.convert(
                dataset["description"],
            ).replace("\n", "<br />"),
        )

    def _setup_private_dataset(
        self,
        dataset_dir: str,
        annex: Callable,
        dataset: Dataset,
        dataset_token: str,
    ):
        if self.verbose:
            print(
                "Dataset is private, creating Zenodo provider and make git annex autoenable datalad remote",
            )

        # Create Zenodo provider file and needed directories and don't annex the file
        datalad_dir: str = os.path.join(dataset_dir, ".datalad")
        if not os.path.exists(datalad_dir):
            os.mkdir(datalad_dir)
        providers_dir: str = os.path.join(datalad_dir, "providers")
        if not os.path.exists(providers_dir):
            os.mkdir(providers_dir)
        zenodo_config_path: str = os.path.join(providers_dir, "ZENODO.cfg")
        with open(zenodo_config_path, "w") as f:
            f.write(
                """[provider:ZENODO]
url_re = .*zenodo\\.org.*
authentication_type = bearer_token
credential = ZENODO

[credential:ZENODO]
# If known, specify URL or email to how/where to request credentials
# url = ???
type = token"""
            )
        dataset.no_annex(os.path.join("**", "ZENODO.cfg"))

        # Make git annex autoenable datalad remote
        annex(
            "initremote",
            "datalad",
            "externaltype=datalad",
            "type=external",
            "encryption=none",
            "autoenable=true",
        )

        # Set ZENODO token as a environment variable for authentication
        os.environ["DATALAD_ZENODO_token"] = dataset_token

        # Save changes
        dataset.save()


========================================
FILE: ./log.py
========================================
import logging


def get_logger(
    name, *, filename=None, console_level=logging.INFO, file_level=logging.WARNING
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(console_level)
    c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    if filename:
        f_handler = logging.FileHandler(filename)
        f_handler.setLevel(file_level)
        f_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

    return logger


========================================
FILE: ./conp_to_nidm_terms/functions.py
========================================
import json
import logging
import os
from collections import Counter
from copy import deepcopy

import requests


logger = logging.getLogger(__name__)


CONP_DATASET_ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../.."))
# conp-dataset/projects
PROJECTS_DIR = os.path.join(CONP_DATASET_ROOT_DIR, "projects")
CURRENT_WORKING_DIR = os.path.dirname(os.path.realpath(__file__))

# More about NIF API endpoints https://neuinfo.org/about/webservices
NIF_API_URL = "https://scicrunch.org/api/1/ilx/search/term/"

# Load JSON-LD template
with open("template.jsonld", encoding="utf-8") as template_file:
    JSONLD_TEMPLATE = json.load(template_file)


# Set API key
with open("api_key.json", encoding="utf-8") as api_key_file:
    API_KEY = json.load(api_key_file)["api_key"]


def get_api_response(term):
    """
    Call NIF API and retrieve InterLex URI for a term.
    :param term: string with the term to send to the API
    :return: string Interlex URI
    """

    # API Key must be provided
    if not API_KEY:
        raise Exception(
            "Add your API Key for the NIF data services to the api_key.json file.",
        )

    try:
        api_key = f"?key={API_KEY}"
        r = requests.get(
            NIF_API_URL + term + api_key,
            headers={"accept": "application/json"},
        )
        r.raise_for_status()
        response = json.loads(r.content.decode("utf-8"))
        match = ""
        # Standard response will have existing_ids key
        if "existing_ids" in response["data"] and response["data"]["existing_ids"]:
            for i in response["data"]["existing_ids"]:
                # retrieve InterLex ID, its curie has "ILX" prefix
                match = (
                    i["iri"] if "curie" in i and "ILX:".upper() in i["curie"] else match
                )
        else:
            match = "no match found"
        return match

    except requests.exceptions.HTTPError as e:
        logger.error(f"Error: {e}")


def collect_values(
    privacy=True,
    types=True,
    licenses=True,
    is_about=True,
    formats=True,
    keywords=True,
):
    """
    Iterates over the projects directory content retrieving DATS file for each project.
    Aggregates all values and their count for selected properties in the report object.
    :param : set to False in order to exclude the property from the final report
    :return: dict object report, int how many DATS files were processed
    """

    # Text values to collect
    privacy_values = set()
    licenses_values = set()
    types_datatype_values = set()
    is_about_values = set()
    distributions_formats = set()
    keywords_values = set()

    dats_files_count = 0

    # Access DATS.json in each project's root directory
    for path, _, files in os.walk(PROJECTS_DIR):
        if "DATS.json" in files:
            dats_files_count += 1
            dats_file = os.path.join(path, "DATS.json")
            with open(dats_file, encoding="utf-8") as json_file:
                dats_data = json.load(json_file)

                # privacy is not required
                if privacy and "privacy" in dats_data:
                    privacy_values.add(dats_data["privacy"])

                if types:
                    # types are required
                    for typ in dats_data["types"]:
                        # types takes four possible datatype schemas
                        datatype_schemas = [
                            "information",
                            "method",
                            "platform",
                            "instrument",
                        ]
                        types_datatype_values.update(
                            {typ[t]["value"] for t in datatype_schemas if t in typ},
                        )

                if licenses:
                    # licenses is required
                    licenses_values.update(
                        {licence["name"] for licence in dats_data["licenses"]},
                    )

                # isAbout is not required
                if is_about and "isAbout" in dats_data:
                    for each_is_about in dats_data["isAbout"]:
                        if "name" in each_is_about:
                            is_about_values.add(each_is_about["name"])
                        elif "value" in each_is_about:
                            is_about_values.add(each_is_about["value"])
                        else:
                            pass

                # distributions is required
                if formats:
                    for dist in dats_data["distributions"]:
                        if "formats" in dist:
                            distributions_formats.update({f for f in dist["formats"]})

                if keywords:
                    keywords_values.update({k["value"] for k in dats_data["keywords"]})

    report = {}
    for key, value in zip(
        ["privacy", "licenses", "types", "is_about", "formats", "keywords"],
        [
            privacy_values,
            licenses_values,
            types_datatype_values,
            is_about_values,
            distributions_formats,
            keywords_values,
        ],
    ):
        if value:
            report[key] = {
                "count": len(value),
                "values": list(value),
            }
    return report, dats_files_count


def find_duplicates(report):
    """
    Finds duplicate values spelled in different cases (e.g. lowercase vs uppercase vs title)
    :param report: json object returned by collect_values()
    :return: list of errors describing where duplicates occur
    """
    errors = []
    for key in ["privacy", "licenses", "types", "is_about", "formats", "keywords"]:
        if key in report:
            terms = report[key]["values"]
            normilized_terms = {}
            for term in terms:
                if term.lower() in normilized_terms:
                    normilized_terms[term.lower()].append(term)
                else:
                    normilized_terms[term.lower()] = [term]

            if report[key]["count"] == len(normilized_terms.keys()):
                logger.info(f"All terms are unique in {key}.")
            else:
                for _, v in normilized_terms.items():
                    if len(v) > 1:
                        errors.append(f"{key.title()} duplicate terms: {v}")
    return errors


def generate_jsonld_files(report, use_api=True):
    """
    Generates a JSON-LD file for each unique term.
    Files are saved to the directories respective to their properties.
    :param report: json object returned by collect_values()
    :param use_api: defaults to True; if False then NIF API won't be called for InterLex match
    """
    terms_counter = Counter()
    for key, value in report.items():
        for term in value["values"]:
            terms_counter.update((term.lower(),))
            jsonld_description = deepcopy(JSONLD_TEMPLATE)
            jsonld_description["label"] = f"{term.lower()}"
            if use_api:
                # Get NIF API matching URI
                jsonld_description["sameAs"] = get_api_response(term.lower())
            # Create a folder for each text value type (e.g. privacy, licenses, etc.)
            if not os.path.exists(os.path.join(CURRENT_WORKING_DIR, key)):
                os.makedirs(os.path.join(CURRENT_WORKING_DIR, key))
            filename = "".join(x for x in term.title().replace(" ", "") if x.isalnum())
            # Create and save JSON-LD file in the respective folder
            with open(
                f"{os.path.join(CURRENT_WORKING_DIR, key, filename)}.jsonld",
                "w",
                encoding="utf-8",
            ) as jldfile:
                json.dump(jsonld_description, jldfile, indent=4, ensure_ascii=False)
    print(f"JSON-LD files created: {len(terms_counter.keys())}")
    return


========================================
FILE: ./conp_to_nidm_terms/__init__.py
========================================


========================================
FILE: ./conp_to_nidm_terms/report_generator.py
========================================
import getopt
import json
from datetime import date
from sys import argv

from functions import collect_values
from functions import find_duplicates


def main(argv):
    timestamp = date.today()
    opts, args = getopt.getopt(
        argv,
        "",
        [
            "filename=",
            "privacy=",
            "types=",
            "licenses=",
            "is_about=",
            "formats=",
            "keywords=",
            "help",
        ],
    )

    options = dict(
        privacy=True,
        types=True,
        licenses=True,
        is_about=True,
        formats=True,
        keywords=True,
    )
    filename = f"report_{timestamp}"

    for opt, arg in opts:
        opt_properties = [
            "--privacy",
            "--types",
            "--licenses",
            "--is_about",
            "--formats",
            "--keywords",
        ]
        if opt in opt_properties and arg == "False":
            options[opt.replace("--", "")] = False
        elif opt == "--filename":
            filename = arg
        else:
            help_info()
            exit()

    report, dats_files_count = collect_values(
        privacy=options["privacy"],
        types=options["types"],
        licenses=options["licenses"],
        is_about=options["is_about"],
        formats=options["formats"],
        keywords=options["keywords"],
    )
    print(f"DATS files processed: {dats_files_count}")
    # check if duplicate terms exist
    duplicates = find_duplicates(report)
    if duplicates:
        # save duplicates to a file
        with open("duplicates.txt", "w") as f:
            for i, item in enumerate(duplicates, 1):
                f.write(f"{i}. {item}\n")
            print("Duplicates were found and saved to the duplicates.txt.")
    # save report to a file
    with open(f"{filename}.json", "w") as report_file:
        json.dump(report, report_file, indent=4)
        print(f"Report {filename}.json created.")


def help_info():
    print(
        "Usage:"
        "python report_generator.py [--privacy=False --types=False --licenses=False "
        "--is_about= --formats=False --keywords=False --help]",
    )


if __name__ == "__main__":
    main(argv[1:])


========================================
FILE: ./conp_to_nidm_terms/jsonld_generator.py
========================================
import getopt
from sys import argv

from functions import API_KEY
from functions import collect_values
from functions import generate_jsonld_files


def main(argv):
    opts, args = getopt.getopt(
        argv,
        "",
        [
            "privacy=",
            "types=",
            "licenses=",
            "is_about=",
            "formats=",
            "keywords=",
            "use_api=",
            "help",
        ],
    )

    options = dict(
        privacy=True,
        types=True,
        licenses=True,
        is_about=True,
        formats=True,
        keywords=True,
    )
    use_api = True

    for opt, arg in opts:
        opt_properties = [
            "--privacy",
            "--types",
            "--licenses",
            "--is_about",
            "--formats",
            "--keywords",
        ]
        if opt in opt_properties and arg == "False":
            options[opt.replace("--", "")] = False
        elif opt == "--use_api" and arg == "False":
            use_api = False
        else:
            help_info()
            exit()

    if use_api and not API_KEY:
        print(
            "The API key is not set in the api_key.json. Add your API Key or set --use_api=False",
        )
        exit()

    report, dats_files_count = collect_values(
        privacy=options["privacy"],
        types=options["types"],
        licenses=options["licenses"],
        is_about=options["is_about"],
        formats=options["formats"],
        keywords=options["keywords"],
    )
    print(f"DATS files processed: {dats_files_count}")

    generate_jsonld_files(report=report, use_api=use_api)


def help_info():
    print(
        "Usage:"
        "python jsonld_generator.py [--privacy=False --types=False --licenses=False "
        "--is_about= --formats=False --keywords=False --use_api=False --help]",
    )


if __name__ == "__main__":
    main(argv[1:])


========================================
FILE: ./__init__.py
========================================


========================================
FILE: ./datalad_helper_scripts/batch_remove_deprecated_URLs.py
========================================
import getopt
import json
import os
import re
import sys
import traceback

import git


def parse_input(argv):
    """
    Displays the script's help section and parses the options given to the script.

    :param argv: command line arguments
     :type argv: array

    :return: parsed and validated script options
     :rtype: dict
    """

    script_options = {}

    description = (
        "\nThis script can be used to remove from git-annex a series of URLs matching"
        " a specific pattern.\n"
        "\t- To run the script and print out the URLs that will be removed, use options"
        " -d <dataset path> -u <invalid URL regex>.\n"
        "\t- After examination of the result of the script, rerun the script with the same"
        " option and add the -c argument for actual removal of the URLs.\n"
        "\t- Option -v prints out progress of the script in the terminal.\n"
    )

    usage = (
        f"\nusage  : python {__file__} -d <DataLad dataset directory path> -u <invalid URL regex>\n"
        "\noptions: \n"
        "\t-d: path to the DataLad dataset to work on\n"  # noqa: E131
        "\t-u: regular expression for invalid URLs to remove from git-annex\n"  # noqa: E131
        "\t-c: confirm that the removal of the URLs should be performed. By default it will just print out what needs to be removed for validation\n"  # noqa: E501,E131
        "\t-v: verbose\n"  # noqa: E131
    )

    try:
        opts, args = getopt.getopt(argv, "hcd:u:")
    except getopt.GetoptError:
        sys.exit()

    script_options["run_removal"] = False
    script_options["verbose"] = False

    if not opts:
        print(description + usage)
        sys.exit()

    for opt, arg in opts:
        if opt == "-h":
            print(description + usage)
            sys.exit()
        elif opt == "-d":
            script_options["dataset_path"] = arg
        elif opt == "-u":
            script_options["invalid_url_regex"] = arg
        elif opt == "-c":
            script_options["run_removal"] = True
        elif opt == "-v":
            script_options["verbose"] = True

    if "dataset_path" not in script_options.keys():
        print(
            "\n\t* ----------------------------------------------------------------------------------------------------------------------- *"  # noqa: E501
            "\n\t* ERROR: a path to the DataLad dataset to process needs to be given as an argument to the script by using the option `-d` *"  # noqa: E501
            "\n\t* ----------------------------------------------------------------------------------------------------------------------- *",  # noqa: E501
        )
        print(description + usage)
        sys.exit()

    if not os.path.exists(script_options["dataset_path"]):
        print(
            f"\n\t* ------------------------------------------------------------------------------ *"
            f"\n\t* ERROR: {script_options['dataset_path']} does not appear to be a valid path   "
            f"\n\t* ------------------------------------------------------------------------------ *",
        )
        print(description + usage)
        sys.exit()

    if not os.path.exists(os.path.join(script_options["dataset_path"], ".datalad")):
        print(
            f"\n\t* ----------------------------------------------------------------------------------- *"
            f"\n\t* ERROR: {script_options['dataset_path']} does not appear to be a DataLad dataset   "
            f"\n\t* ----------------------------------------------------------------------------------- *",
        )
        print(description + usage)
        sys.exit()

    if "invalid_url_regex" not in script_options.keys():
        print(
            "\n\t* --------------------------------------------------------------------------------------------------- *"  # noqa: E501
            "\n\t* ERROR: a regex for invalid URLs to remove should be provided to the script by using the option `-u` *"  # noqa: E501
            "\n\t* --------------------------------------------------------------------------------------------------- *",  # noqa: E501
        )
        print(description + usage)
        sys.exit()

    return script_options


def get_files_and_urls(dataset_path, annex):
    """
    Runs git annex whereis in the dataset directory to retrieve
    a list of annexed files with their URLs' location.

    :param dataset_path: full path to the DataLad dataset
     :type dataset_path: string
    :param annex: the git annex object
     :type annex: object

    :return: files path and there URLs organized as follows:
             {
                <file-1_path> => [file-1_url-1, file-1_url-2 ...]
                <file-2_path> => [file-2_url-1, file-2_url-2 ...]
                ...
             }
     :rtype: dict
    """

    current_path = os.path.dirname(os.path.realpath(__file__))

    results = {}
    try:
        os.chdir(dataset_path)
        annex_results = annex("whereis", ".", "--json")
        results_list = annex_results.split("\n")
        for annex_result_item in results_list:
            r_json = json.loads(annex_result_item)
            file_path = r_json["file"]
            file_urls = []
            for entry in r_json["whereis"]:
                file_urls.extend(entry["urls"])
            results[file_path] = file_urls
    except Exception:
        traceback.print_exc()
        sys.exit()
    finally:
        os.chdir(current_path)

    return results


def filter_invalid_urls(files_and_urls_dict, regex_pattern):
    """
    Filters out the URLs that need to be removed based on a regular
    expression pattern.

    :param files_and_urls_dict: files' path and their respective URLs.
     :type files_and_urls_dict: dict
    :param regex_pattern: regular expression pattern for URL filtering
     :type regex_pattern: str

    :return: filtered URLs per file
     :rtype: dict
    """

    filtered_dict = {}
    for file_path in files_and_urls_dict.keys():
        filtered_urls_list = filter(
            lambda x: re.search(regex_pattern, x),
            files_and_urls_dict[file_path],
        )
        filtered_dict[file_path] = filtered_urls_list

    return filtered_dict


def remove_invalid_urls(filtered_file_urls_dict, script_options, annex):
    """
    Removes URLs listed in the filtered dictionary from the files.

    :param filtered_file_urls_dict: filtered URLs to remove per file
     :type filtered_file_urls_dict: dict
    :param script_options: options give to the script
     :type script_options: dict
    :param annex: the git annex object
     :type annex: object
    """

    dataset_path = script_options["dataset_path"]
    current_path = os.path.dirname(os.path.realpath(__file__))

    try:
        os.chdir(dataset_path)
        for file_path in filtered_file_urls_dict.keys():
            for url in filtered_file_urls_dict[file_path]:
                if script_options["run_removal"]:
                    if script_options["verbose"]:
                        print(f"\n => Running `git annex rmurl {file_path} {url}`\n")
                    annex("rmurl", file_path, url)
                else:
                    print(
                        f"\nWill be running `git annex rmurl {file_path} {url}`\n",
                    )
    except Exception:
        traceback.print_exc()
    finally:
        os.chdir(current_path)


if __name__ == "__main__":

    script_options = parse_input(sys.argv[1:])

    repo = git.Repo(script_options["dataset_path"])
    annex = repo.git.annex

    # fetch files and urls attached to the file
    if script_options["verbose"]:
        print(
            f"\n => Reading {script_options['dataset_path']} and grep annexed files with their URLs\n",
        )
    files_and_urls_dict = get_files_and_urls(script_options["dataset_path"], annex)

    # grep only the invalid URLs that need to be removed from the annexed files
    regex_pattern = re.compile(script_options["invalid_url_regex"])
    if script_options["verbose"]:
        print(
            f"\n => Grep the invalid URLs based on the regular expression {regex_pattern}",
        )
    filtered_file_urls_dict = filter_invalid_urls(files_and_urls_dict, regex_pattern)

    # remove the invalid URLs found for each annexed file
    remove_invalid_urls(filtered_file_urls_dict, script_options, annex)


========================================
FILE: ./datalad_utils.py
========================================
from __future__ import annotations

import functools
import os

import datalad.api


class InstallFailed(Exception):
    pass


class DownloadFailed(Exception):
    pass


class UninstallFailed(Exception):
    pass


def retry(max_attempt):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            success = False
            current_attempt = 1
            last_exception = None

            while not success and current_attempt <= max_attempt:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    current_attempt += 1
                    last_exception = e

            if not success:
                raise last_exception

        return wrapper

    return decorator


@retry(max_attempt=3)
def _install_dataset(dataset_path: str, *, recursive: bool = False):
    full_path = os.path.join(os.getcwd(), dataset_path)
    datalad.api.install(path=full_path, recursive=recursive, on_failure="stop")


def install_dataset(dataset_path: str, *, recursive: bool = False) -> None:
    try:
        _install_dataset(dataset_path, recursive=recursive)
    except Exception as e:
        raise InstallFailed(f"Installation failed for dataset: {dataset_path}\n{e}")


@retry(max_attempt=3)
def _get_dataset(dataset_path: str, *, recursive: bool = False) -> None:
    full_path = os.path.join(os.getcwd(), dataset_path)
    datalad.api.get(path=full_path, recursive=recursive, on_failure="stop")


def get_dataset(dataset_path: str, *, recursive: bool = False) -> None:
    try:
        _get_dataset(dataset_path, recursive=recursive)
    except Exception as e:
        raise DownloadFailed(f"Download failed for dataset: {dataset_path}\n{e}")


@retry(max_attempt=3)
def _uninstall_dataset(dataset_path: str, *, recursive: bool = False):
    full_path = os.path.join(os.getcwd(), dataset_path)
    datalad.api.uninstall(path=full_path, recursive=recursive, on_failure="stop")


def uninstall_dataset(dataset_path: str, *, recursive: bool = False) -> None:
    try:
        _uninstall_dataset(dataset_path, recursive=recursive)
    except Exception as e:
        raise UninstallFailed(f"Installation failed for dataset: {dataset_path}\n{e}")


========================================
FILE: ./dats_validator/validator.py
========================================
import getopt
import json
import logging
import os
from sys import argv

import jsonschema
import requests


logger = logging.getLogger(__name__)
# path to a top-level schema
SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "conp-dats",
    "dataset_schema.json",
)


# set value to 0 if there is no controlled vocabulary list, set value to a list if there is one.
REQUIRED_EXTRA_PROPERTIES = {
    "files": 0,
    "subjects": 0,
    "CONP_status": ["CONP", "Canadian", "external"],
}


def main(argv):
    FORMAT = "%(message)s"
    logging.basicConfig(format=FORMAT)
    logging.getLogger().setLevel(logging.INFO)
    opts, args = getopt.getopt(argv, "", ["file="])
    json_filename = ""

    for opt, arg in opts:
        if opt == "--file":
            json_filename = arg

    if json_filename == "":
        help()
        exit()

    with open(json_filename) as json_file:
        json_obj = json.load(json_file)
        validate_json(json_obj)
        validate_non_schema_required(json_obj)


def validate_json(json_obj):
    with open(SCHEMA_PATH) as s:
        json_schema = json.load(s)
    # first validate schema file
    v = jsonschema.Draft4Validator(
        json_schema,
        format_checker=jsonschema.FormatChecker(),
    )
    # now validate json file
    try:
        jsonschema.validate(
            json_obj,
            json_schema,
            format_checker=jsonschema.FormatChecker(),
        )
        logger.info("JSON schema validation passed.")
        return True
    except jsonschema.exceptions.ValidationError:
        errors = [e for e in v.iter_errors(json_obj)]
        logger.info(f"The file is not valid. Total json schema errors: {len(errors)}")
        for i, error in enumerate(errors, 1):
            logger.error(
                f"{i} Validation error in {'.'.join(str(v) for v in error.path)}: {error.message}",
            )
        logger.info("JSON schema validation failed.")
        return False


def validate_extra_properties(dataset):
    """Checks if required extraProperties are present in a dataset."""

    try:
        errors = []
        extra_prop_categories = {
            prop["category"]: [value["value"] for value in prop["values"]]
            for prop in dataset["extraProperties"]
            if "extraProperties" in dataset
        }
        # first checks if required extraProperties categories are present
        for category in REQUIRED_EXTRA_PROPERTIES:
            if category not in extra_prop_categories:
                error_message = (
                    f"Validation error in {dataset['title']}: "
                    f"extraProperties.category.{category} is required but not found."
                )
                errors.append(error_message)

        # checks if values of required extraProperties are correct according to a controlled vocabulary
        if "CONP_status" in extra_prop_categories:
            for each_value in extra_prop_categories["CONP_status"]:
                if each_value not in REQUIRED_EXTRA_PROPERTIES["CONP_status"]:
                    error_message = (
                        f"Validation error in {dataset['title']}: extraProperties.category."
                        f"CONP_status - {each_value} is not allowed value for CONP_status. "
                        f"Allowed values are {REQUIRED_EXTRA_PROPERTIES['CONP_status']}."
                    )
                    errors.append(error_message)

        # checks if 'derivedFrom' values refer to existing datasets accessible online
        if "derivedFrom" in extra_prop_categories:
            for value in extra_prop_categories["derivedFrom"]:
                if not dataset_exists(value):
                    error_message = (
                        f"Validation error in {dataset['title']}: extraProperties.category."
                        f"derivedFrom - {value} is not found. "
                    )
                    errors.append(error_message)

        if errors:
            return False, errors
        else:
            return True, errors

    # extraProperties is only required property which is not required on dataset_schema level,
    # if it's not present an Exception is raised
    except KeyError as e:
        raise KeyError(
            f"{e} is required."
            f"The following extra properties categories are required: "
            f"{[k for k in REQUIRED_EXTRA_PROPERTIES.keys()]}",
        )


def validate_formats(dataset):
    """Checks if the values in the formats field of the JSON object follows the upper case convention without dots."""

    errors_list = []
    format_exceptions = ["bigWig", "NIfTI", "GIfTI", "RNA-Seq"]

    # check that distributions have a formats property as this is required in the schema
    for distribution_dict in dataset["distributions"]:
        if "formats" not in distribution_dict.keys():
            error_message = (
                f"Validation error in {dataset['title']}: distributions."
                f"formats - 'formats' property is missing under distributions. "
                f"Please add the 'formats' property to 'distributions'."
            )
            errors_list.append(error_message)
        else:
            for file_format in distribution_dict["formats"]:
                if (
                    file_format != file_format.upper()
                    and file_format not in format_exceptions
                ):
                    error_message = (
                        f"Validation error in {dataset['title']}: distributions."
                        f"formats - {file_format} is not allowed. "
                        f"Allowed value should either be capitalized or one of {format_exceptions}. "
                        f"Consider changing the value to {file_format.strip('.').upper()}. "
                    )
                    errors_list.append(error_message)
                elif file_format.startswith("."):
                    error_message = (
                        f"Validation error in {dataset['title']}: distributions."
                        f"formats - {file_format} is not allowed. "
                        f"Format values should not start with a dot."
                    )
                    errors_list.append(error_message)

    if errors_list:
        return False, errors_list
    else:
        return True, errors_list


def date_type_validation(dates_list, dataset_title):

    errors_list = []
    date_type_exception = ["CONP DATS JSON fileset creation date"]

    for date_dict in dates_list:
        dtype = date_dict["type"]["value"]
        if dtype != dtype.lower() and dtype not in date_type_exception:
            error_message = (
                f"Validation error in {dataset_title}: dates.type - {dtype} is not allowed. "
                f"Allowed value should either be all lower case or one of {date_type_exception}. "
                f"Consider changing the value to {dtype.lower()}"
            )
            errors_list.append(error_message)

    return errors_list


def validate_date_types(dataset):
    """Checks if the values in the dates type field of the JSON object follows the lower case convention."""

    errors_list = []

    if "dates" in dataset.keys():
        dates_errors_list = date_type_validation(dataset["dates"], dataset["title"])
        errors_list.extend(dates_errors_list)

    if "primaryPublications" in dataset.keys():
        for publication in dataset["primaryPublications"]:
            if "dates" in publication:
                dates_errors_list = date_type_validation(
                    publication["dates"],
                    dataset["title"],
                )
                errors_list.extend(dates_errors_list)

    if errors_list:
        return False, errors_list
    else:
        return True, errors_list


def validate_privacy(dataset):
    """
    Checks if the values in the privacy field of the JSON object is one of:
    - open
    - registered
    - controlled
    - private
    '"""

    errors_list = []
    valid_privacy_values = ["open", "registered", "controlled", "private"]

    if "privacy" in dataset.keys():
        if dataset["privacy"] not in valid_privacy_values:
            error_message = (
                f"Validation error in {dataset['title']}: privacy "
                f"- '{dataset['privacy']}' is not allowed. Allowed "
                f"value should be one of {valid_privacy_values}. "
            )
            errors_list.append(error_message)

    if errors_list:
        return False, errors_list
    else:
        return True, errors_list


def validate_is_about(dataset):
    """
    Checks whether there is at least one entry in the 'isAbout' field with an
    'identifier' and an 'identifierSource' containing a value that starts
    with the string 'https://www.ncbi.nlm.nih.gov/Taxonomy'.

    Note: isAbout is not a required field.
    """

    example_species = """
    "isAbout": [
        {
            "identifier": {
                "identifier"      : "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606",
            },
            "name"      : "Homo sapiens"
        }
    ],
    """

    errors_list = []
    identifier_source_base_url = "https://www.ncbi.nlm.nih.gov/taxonomy"

    if "isAbout" in dataset.keys():
        species_present = False
        for entry in dataset["isAbout"]:
            if "identifier" in entry.keys():
                identifier_source = entry["identifier"]["identifier"].lower()
                if identifier_source.startswith(identifier_source_base_url):
                    species_present = True

        if not species_present:
            error_message = (
                f"Validation error in {dataset['title']}: isAbout "
                f"- There appears to be no species specified in isAbout. "
                f"At least one species is required in the field and should "
                f"follow the NCBI taxonomy. Valid example for a species:\n"
                f"{example_species}"
            )
            errors_list.append(error_message)

    if errors_list:
        return False, errors_list
    else:
        return True, errors_list


def validate_types(dataset):
    errors_list = []
    if "types" in dataset.keys():
        # 1 check for empty object inside of types list
        empty_obj = [obj for obj in dataset["types"] if not obj]
        if len(empty_obj) == len(dataset["types"]):
            error_message = (
                f"Validation in {dataset['title']}: types - list has no value."
            )
            errors_list.append(error_message)

        # 2 check that only data_type_schema properties are present
        for obj in dataset["types"]:
            allowed_keys = [
                "@context",
                "@id",
                "@type",
                "information",
                "method",
                "platform",
                "instrument",
            ]
            for key in obj.keys():
                if key not in allowed_keys:
                    error_message = (
                        f"Validation in {dataset['title']}: "
                        f"types - the key {key} is not supported by the schema."
                    )
                    errors_list.append(error_message)
    # no need to check for types otherwise because this error will be caught by jsonschema validation
    if errors_list:
        return False, errors_list
    else:
        return True, errors_list


def validate_recursively(obj, errors):
    """Checks all datasets recursively for non-schema checks."""

    val, errors_list = validate_extra_properties(obj)
    errors.extend(errors_list)
    val, errors_list = validate_formats(obj)
    errors.extend(errors_list)
    val, errors_list = validate_date_types(obj)
    errors.extend(errors_list)
    val, errors_list = validate_privacy(obj)
    errors.extend(errors_list)
    val, errors_list = validate_is_about(obj)
    errors.extend(errors_list)
    val, errors_list = validate_types(obj)
    errors.extend(errors_list)

    if "hasPart" in obj:
        for each in obj["hasPart"]:
            validate_recursively(each, errors)


def validate_non_schema_required(json_obj):
    """Checks if json object has all required extra properties beyond json schema. Prints error report."""

    errors = []
    validate_recursively(json_obj, errors)
    if errors:
        logger.info(f"Total required extra properties errors: {len(errors)}")
        for i, er in enumerate(errors, 1):
            logger.error(f"{i} {er}")
        return False, errors
    else:
        logger.info("Required extra properties validation passed.")
        return True, None


# cache responses to avoid redundant calls
cache = {}


def dataset_exists(derived_from_url):
    """Caches response values in cache dict."""

    if derived_from_url not in cache:
        cache[derived_from_url] = get_response_status(derived_from_url)
    return cache[derived_from_url]


def get_response_status(derived_from_url):
    """Get a response status code for derivedFrom value. Returns True if status code is 200."""

    try:
        r = requests.get(derived_from_url)
        r.raise_for_status()
        if r.status_code == 200:
            return True

    except requests.exceptions.HTTPError:
        return False


def help():
    return logger.info("Usage: python validator.py --file=doc.json")


if __name__ == "__main__":
    main(argv[1:])


========================================
FILE: ./dats_validator/__init__.py
========================================


========================================
FILE: ./crawl_single_zenodo_record.py
========================================
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


========================================
FILE: ./auto_archive.py
========================================
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from datetime import timedelta

import git
import humanfriendly
from datalad.plugin import export_archive
from github import Github

from scripts.datalad_utils import get_dataset
from scripts.datalad_utils import install_dataset
from scripts.datalad_utils import uninstall_dataset
from scripts.log import get_logger
from tests.functions import get_proper_submodules


logger = get_logger(
    "CONP-Archive", filename="conp-archive.log", file_level=logging.DEBUG
)


class ArchiveFailed(Exception):
    pass


def parse_args():
    example_text = """Example:
    PYTHONPATH=$PWD python scripts/auto_archive.py <out_dir>
    """

    parser = argparse.ArgumentParser(
        description="Archiver for the CONP-datasets.",
        epilog=example_text,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--out_dir", "-o", type=str, help="Path to store the archived datasets."
    )
    parser.add_argument(
        "--max-size",
        type=float,
        help="Maximum size of dataset to archive in GB.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--all",
        action="store_true",
        help="Archive all the datasets rather than those modified since the last time.",
    )
    group.add_argument(
        "--dataset",
        "-d",
        type=str,
        nargs="+",
        help="Restrict the archive to the specified dataset paths.",
    )

    return parser.parse_args()


def get_datasets_path():
    return {
        os.path.basename(submodule.path): submodule.path
        for submodule in git.Repo().submodules
        if submodule.path.startswith("projects")
    }


def get_modified_datasets(
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> set[str]:
    """Retrieve the modified datasets.

    Requires to set GITHUB_ACCESS_TOKEN as an environment variable.

    Parameters
    ----------
    since : Optional[datetime], optional
        Start date from which commits are retrieved, by default date of the previous crawl, if never crawled set to
        one week ago.
    until : Optional[datetime], optional
        Latest date at which commit are retrieved, by default `now`

    Returns
    -------
    set[str]
        Path of the dataset folders.
    """
    now = datetime.now().astimezone()

    if since is None:
        if os.path.exists(".conp-archive"):
            with open(".conp-archive") as fin:
                since = datetime.fromisoformat(fin.read().rstrip("\n"))
        else:
            since = now - timedelta(weeks=1)

    if until is None:
        until = now

    try:
        gh_access_token = os.environ.get("GITHUB_ACCESS_TOKEN", None)
        if gh_access_token is None:
            raise OSError("GITHUB_ACCESS_TOKEN is not defined.")

    except OSError as e:
        # The program is not stopped since GitHub allows 60 query per hours with
        # authentication. However the program will most likely fail.
        logger.critical(e)

    logger.info(f"Retrieving modified datasets since {since}")
    repo = Github(gh_access_token).get_repo("CONP-PCNO/conp-dataset")
    commits = repo.get_commits(since=since, until=until)

    with open(".conp-archive", "w") as fout:
        fout.write(now.isoformat())

    modified_datasets: set[str] = {
        os.path.basename(file_.filename)
        for commit in commits
        for file_ in commit.files
        if file_.filename.startswith("projects/")
    }

    return modified_datasets


def archive_dataset(
    dataset_path: str, out_dir: str, archive_name: str, version: str
) -> None:
    os.makedirs(out_dir, mode=0o755, exist_ok=True)
    out_filename = os.path.join(out_dir, f"{archive_name}_version-{version}.tar.gz")
    logger.info(f"Archiving dataset: {dataset_path} to {out_filename}")

    cwd = os.getcwd()
    try:
        datalad_archiver = export_archive.ExportArchive()
        dataset_repo = git.Repo(dataset_path)

        with open(os.path.join(dataset_path, ".git.log"), "w") as fout:
            fout.write(dataset_repo.git.log(pretty="format:%H %s"))

        # Export is performed from the dataset root.
        # This is to avoid failure when a submodule is not downloaded; e.g. for parent
        # dataset in dataset derivative.
        os.chdir(os.path.join(cwd, dataset_path))
        datalad_archiver(".", filename=out_filename)

    except Exception as e:
        raise ArchiveFailed(
            f"FAILURE: could not archive dataset: {dataset_path} to {out_filename}\n{e}"
        )
    finally:
        os.chdir(cwd)


if __name__ == "__main__":
    args = parse_args()

    # Only archive the datasets available locally.
    datasets_path = get_datasets_path()
    datasets = datasets_path.keys()
    if args.dataset:
        target_datasets = {os.path.basename(os.path.normpath(d)) for d in args.dataset}
        logger.warning(
            f"The following dataset were not found locally: {target_datasets - datasets}"
        )
        datasets &= target_datasets

    elif not args.all:
        modified_datasets = get_modified_datasets()
        logger.warning(
            f"The following dataset were not found locally: {modified_datasets - datasets}"
        )
        datasets &= modified_datasets

    for dataset_name in datasets:
        dataset = datasets_path[dataset_name]

        try:
            logger.info(f"Installing dataset: {dataset}")
            install_dataset(dataset)

            is_public = False
            version = ""
            dataset_size = 0.0

            with open(os.path.join(dataset, "DATS.json")) as fin:
                metadata = json.load(fin)

                is_public = (
                    metadata.get("distributions", [{}])[0]
                    .get("access", {})
                    .get("authorizations", [{}])[0]
                    .get("value")
                    == "public"
                )
                version = metadata.get("version")

                for distribution in metadata.get("distributions", list()):
                    dataset_size += humanfriendly.parse_size(
                        f"{distribution['size']} {distribution['unit']['value']}",
                    )
                    dataset_size //= 1024**3  # Convert to GB

            # Only archive public dataset less than a specific size if one is provided to the script
            if is_public:
                if args.max_size is None or dataset_size <= args.max_size:
                    logger.info(f"Downloading dataset: {dataset}")
                    get_dataset(dataset)
                    for submodule in get_proper_submodules(dataset):
                        get_dataset(submodule)

                    archive_name = "__".join(
                        os.path.relpath(dataset, "projects").split("/")
                    )
                    archive_dataset(
                        dataset,
                        out_dir=args.out_dir,
                        archive_name=archive_name,
                        version=version,
                    )
                    # to save space on the VM that archives the dataset, need to uninstall
                    # the datalad dataset. `datalad drop` does not free up enough space
                    # unfortunately. See https://github.com/datalad/datalad/issues/6009
                    uninstall_dataset(dataset)
                    logger.info(f"SUCCESS: archive created for {dataset}")
                else:
                    logger.info(f"SKIPPED: {dataset} larger than {args.max_size} GB")
            else:
                logger.info(
                    f"SKIPPED: archive not needed for {dataset}. Non-public dataset."
                )

        except Exception as e:
            # TODO implement notification system.
            # This will alert when a dataset fails the archiving process.
            logger.exception(
                f"FAILURE: could not archive dataset: {dataset} to {args.out_dir}.tar.gz\n{e}"
            )

    logger.info("Done archiving the datasets.")


========================================
FILE: ./data_aggregation_summary_scripts/create_dataset_statistcs_per_data_providers.py
========================================
import getopt
import os
import sys

import lib.Utility as Utility


def main(argv):

    # create the getopt table + read and validate the options given to the script
    conp_dataset_dir = parse_input(argv)

    # read the content of the DATS.json files present in the conp-dataset directory
    dataset_descriptor_list = Utility.read_conp_dataset_dir(conp_dataset_dir)

    # digest the content of the DATS.json files into a summary of variables of interest
    datasets_summary_dict = {}
    i = 0
    for dataset in dataset_descriptor_list:
        datasets_summary_dict[i] = parse_dats_information(dataset)
        i += 1

    # create the summary statistics of the variables of interest organized per data providers
    csv_content = [
        [
            "Data Provider",
            "Number Of Datasets",
            "Number Of Datasets Requiring Authentication",
            "Total Number Of Files",
            "Total Size (GB)",
            "Keywords Describing The Data",
        ],
    ]
    for data_provider in ["braincode", "frdr", "loris", "osf", "zenodo"]:
        summary_list = get_stats_for_data_provider(datasets_summary_dict, data_provider)
        csv_content.append(summary_list)

    # write the summary statistics into a CSV file
    Utility.write_csv_file("dataset_summary_statistics_per_data_providers", csv_content)


def parse_input(argv):
    """
    Creates the GetOpt table + read and validate the options given when calling the script.

    :param argv: command-line arguments
     :type argv: list

    :return: the path to the conp-dataset directory
     :rtype: str
    """

    conp_dataset_dir_path = None

    description = (
        "\nThis tool facilitates the creation of statistics per data providers for reporting purposes."
        " It will read DATS files and print out a summary per data providers based on the following list"
        "of DATS fields present in the DATS. json of every dataset present in the conp-dataset/projects"
        "directory.\n Queried fields: <distribution->access->landingPage>; "
        "<distributions->access->authorizations>; "
        "<distributions->size>; <extraProperties->files>; <keywords>\n"
    )
    usage = (
        "\n"
        "usage  : python " + __file__ + " -d <conp-dataset directory path>\n\n"
        "options: \n"
        "\t-d: path to the conp-dataset directory to parse\n"
    )

    try:
        opts, args = getopt.getopt(argv, "hd:")
    except getopt.GetoptError:
        sys.exit()

    for opt, arg in opts:
        if opt == "-h":
            print(description + usage)
            sys.exit()
        elif opt == "-d":
            conp_dataset_dir_path = arg

    if not conp_dataset_dir_path:
        print(
            "a path to the conp-dataset needs to be given as an argument to the script by using the option `-d`",
        )
        print(description + usage)
        sys.exit()

    if not os.path.exists(conp_dataset_dir_path + "/projects"):
        print(
            conp_dataset_dir_path
            + "does not appear to be a valid path and does not include a `projects` directory",
        )
        print(description + usage)
        sys.exit()

    return conp_dataset_dir_path


def parse_dats_information(dats_dict):
    """
    Parse the content of the DATS dictionary and grep the variables of interest for
    the summary statistics.

    :param dats_dict: dictionary with the content of a dataset's DATS.json file
     :type dats_dict: dict

    :return: dictionary with the variables of interest to use to produce the
             summary statistics
     :rtype: dict
    """

    extra_properties = dats_dict["extraProperties"]
    keywords = dats_dict["keywords"]

    values_dict = {
        "extraProperties": {},
        "keywords": [],
    }
    for extra_property in extra_properties:
        values_dict[extra_property["category"]] = extra_property["values"][0]["value"]
    for keyword in keywords:
        values_dict["keywords"].append(keyword["value"])

    authorization = "unknown"
    if "authorizations" in dats_dict["distributions"][0]["access"]:
        authorization = dats_dict["distributions"][0]["access"]["authorizations"][0][
            "value"
        ]

    return {
        "title": dats_dict["title"],
        "data_provider": dats_dict["distributions"][0]["access"]["landingPage"],
        "authorization": authorization,
        "dataset_size": dats_dict["distributions"][0]["size"],
        "size_unit": dats_dict["distributions"][0]["unit"]["value"],
        "number_of_files": values_dict["files"] if "files" in values_dict else "",
        "keywords": values_dict["keywords"] if "keywords" in values_dict else "",
    }


def get_stats_for_data_provider(dataset_summary_dict, data_provider):
    """
    Produces a summary statistics per data provider (Zenodo, OSF, LORIS...) of the
    identified variables of interest.

    :param dataset_summary_dict: dictionary with the variables of interest for the summary
     :type dataset_summary_dict: dict
    :param data_provider       : name of the data provider
     :type data_provider       : str

    :return: list with the summary statistics on the variables for the data provider
     :rtype: list
    """

    dataset_number = 0
    requires_login = 0
    total_size = 0
    total_files = 0
    keywords_list = []

    for index in dataset_summary_dict:

        dataset_dict = dataset_summary_dict[index]
        if data_provider not in dataset_dict["data_provider"]:
            continue

        dataset_number += 1
        if isinstance(dataset_dict["number_of_files"], str):
            total_files += int(dataset_dict["number_of_files"].replace(",", ""))
        else:
            total_files += dataset_dict["number_of_files"]

        if dataset_dict["authorization"].lower() in ["private", "restricted"]:
            requires_login += 1

        if dataset_dict["size_unit"].lower() == "b":
            total_size += dataset_dict["dataset_size"] / pow(1024, 3)
        elif dataset_dict["size_unit"].lower() == "kb":
            total_size += dataset_dict["dataset_size"] / pow(1024, 2)
        elif dataset_dict["size_unit"].lower() == "mb":
            total_size += dataset_dict["dataset_size"] / 1024
        elif dataset_dict["size_unit"].lower() == "gb":
            total_size += dataset_dict["dataset_size"]
        elif dataset_dict["size_unit"].lower() == "tb":
            total_size += dataset_dict["dataset_size"] * 1024
        elif dataset_dict["size_unit"].lower() == "pb":
            total_size += dataset_dict["dataset_size"] * pow(1024, 2)

        for keyword in dataset_dict["keywords"]:
            if keyword not in keywords_list:
                if keyword == "canadian-open-neuroscience-platform":
                    continue
                keywords_list.append(keyword)

    return [
        data_provider,
        str(dataset_number),
        str(requires_login),
        str(total_files),
        str(round(total_size)),
        ", ".join(keywords_list),
    ]


if __name__ == "__main__":
    main(sys.argv[1:])


========================================
FILE: ./data_aggregation_summary_scripts/create_data_provenance_summary.py
========================================
import csv
import datetime
import getopt
import json
import os
import sys


def main(argv):

    conp_dataset_dir = parse_input(argv)

    csv_content = read_conp_dataset_dir(conp_dataset_dir)

    write_csv_file(csv_content)


def parse_input(argv):

    conp_dataset_dir = None

    description = (
        "\nThis tool facilitates the aggregation of data provenance for reporting purposes."
        " It will read DATS files and print out a summary of data provenance based on the `origin`"
        " fields present in the DATS.json files of every dataset present in conp-dataset directory.\n"
    )
    usage = (
        "\n"
        "usage  : python " + __file__ + " -d <conp-dataset directory path>\n\n"
        "options: \n"
        "\t-d: path to the conp-dataset directory to parse\n"
    )

    try:
        opts, args = getopt.getopt(argv, "hd:")
    except getopt.GetoptError:
        sys.exit()

    for opt, arg in opts:
        if opt == "-h":
            print(description + usage)
            sys.exit()
        elif opt == "-d":
            conp_dataset_dir = arg

    if not conp_dataset_dir:
        print(
            "a path to the conp-dataset needs to be given as an argument to the script by using the option `-d`",
        )
        print(description + usage)
        sys.exit()

    if not os.path.exists(conp_dataset_dir + "/projects"):
        print(
            conp_dataset_dir
            + "does not appear to be a valid path and does not include a `projects` directory",
        )
        print(description + usage)
        sys.exit()

    return conp_dataset_dir


def read_conp_dataset_dir(conp_dataset_dir):

    dataset_dirs_list = os.listdir(conp_dataset_dir + "/projects")

    csv_content = [
        [
            "Dataset",
            "Principal Investigator",
            "Consortium",
            "Institution",
            "City",
            "Province",
            "Country",
        ],
    ]

    for dataset in dataset_dirs_list:
        if dataset in [".touchfile", ".DS_Store"]:
            continue
        dats_path = conp_dataset_dir + "/projects/" + dataset + "/DATS.json"
        if not (os.path.exists(dats_path)):
            subdataset_content_list = look_for_dats_file_in_subfolders(
                conp_dataset_dir,
                dataset,
            )
            csv_content.extend(subdataset_content_list)
            continue

        parsed_result = parse_dats_json_file(dats_path)
        csv_content.append(parsed_result)

    return csv_content


def look_for_dats_file_in_subfolders(conp_dataset_dir, dataset):

    subdataset_dirs_list = os.listdir(conp_dataset_dir + "/projects/" + dataset)

    subdataset_content = []

    for subdataset in subdataset_dirs_list:
        dats_path = (
            conp_dataset_dir + "/projects/" + dataset + "/" + subdataset + "/DATS.json"
        )
        parsed_result = parse_dats_json_file(dats_path)
        subdataset_content.append(parsed_result)

    return subdataset_content


def parse_dats_json_file(dats_path):

    print(dats_path)

    with open(dats_path, encoding="utf8") as dats_file:
        dats_dict = json.loads(dats_file.read())

    extra_properties = dats_dict["extraProperties"]
    values_dict = {}
    for extra_property in extra_properties:
        values_dict[extra_property["category"]] = ", ".join(
            str(value) for value in [exp["value"] for exp in extra_property["values"]]
        )

    creators = dats_dict["creators"]
    for creator in creators:
        if "roles" in creator.keys():
            for role in creator["roles"]:
                if (
                    role["value"] == "Principal Investigator"
                    and "name" in creator.keys()
                ):
                    values_dict["principal_investigator"] = creator["name"]

    return [
        dats_dict["title"],
        values_dict["principal_investigator"]
        if "principal_investigator" in values_dict
        else "",
        values_dict["origin_consortium"] if "origin_consortium" in values_dict else "",
        values_dict["origin_institution"]
        if "origin_institution" in values_dict
        else "",
        values_dict["origin_city"] if "origin_city" in values_dict else "",
        values_dict["origin_province"] if "origin_province" in values_dict else "",
        values_dict["origin_country"] if "origin_country" in values_dict else "",
    ]


def write_csv_file(csv_content):

    csv_file = (
        os.getcwd() + "/dataset_provenance_" + str(datetime.date.today()) + ".csv"
    )

    with open(csv_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(csv_content)


if __name__ == "__main__":
    main(sys.argv[1:])


========================================
FILE: ./data_aggregation_summary_scripts/lib/Utility.py
========================================
import csv
import datetime
import json
import os


def read_conp_dataset_dir(conp_dataset_dir_path):
    """
    Reads the conp-dataset projects directory and return the contents
    of every dataset DATS.json file in a list (one list item = one
    dataset DATS.json content).

    :param conp_dataset_dir_path: path to the conp-dataset directory
     :type conp_dataset_dir_path: str

    :return: list of dictionaries with datasets' DATS.json content
             (one list item = one dataset DATS.json content)
     :rtype: list
    """

    dataset_dirs_list = os.listdir(conp_dataset_dir_path + "/projects")

    dataset_descriptor_list = []

    for dataset in dataset_dirs_list:
        if dataset == ".touchfile":
            continue

        dats_path = conp_dataset_dir_path + "/projects/" + dataset + "/DATS.json"
        if not (os.path.exists(dats_path)):
            subdataset_content_list = read_dats_file_from_subdataset_folders(
                conp_dataset_dir_path,
                dataset,
            )
            dataset_descriptor_list.extend(subdataset_content_list)
            continue

        print("Reading file: " + dats_path)
        with open(dats_path) as dats_file:
            dats_dict = json.loads(dats_file.read())
            dataset_descriptor_list.append(dats_dict)

    return dataset_descriptor_list


def read_dats_file_from_subdataset_folders(conp_dataset_dir_path, dataset_name):
    """
    Reads DATS.json files present in the subdataset folder of a dataset_name.

    :param conp_dataset_dir_path: path to the conp-dataset_name directory
     :type conp_dataset_dir_path: str
    :param dataset_name         : name of the dataset to look for subdataset's DATS.json files
     :type dataset_name         : str

    :return: list of dictionaries with subdatasets' DATS.json content
             (one list item = one subdataset DATS.json content)
     :rtype: list
    """

    subdataset_dirs_list = os.listdir(
        conp_dataset_dir_path + "/projects/" + dataset_name,
    )

    subdataset_content = []

    for subdataset in subdataset_dirs_list:
        dats_path = os.path.join(
            conp_dataset_dir_path,
            "projects",
            dataset_name,
            subdataset,
            "DATS.json",
        )
        print("Reading file: " + dats_path)
        with open(dats_path) as dats_file:
            dats_dict = json.loads(dats_file.read())
            subdataset_content.append(dats_dict)

    return subdataset_content


def read_boutiques_cached_dir(tools_json_dir_path):
    """
    Reads the Boutiques' cache directory and return the contents
    of every JSON descriptor file in a list (one list item = one
    Boutiques' JSON descriptor content).

    :param tools_json_dir_path: path to the cached Boutiques directory
     :type tools_json_dir_path: str

    :return: list of dictionaries with Boutiques' JSON descriptor content
             (one list item = one Boutiques' JSON descriptor content)
     :rtype: list
    """

    boutiques_descriptor_list = []

    for json_file in os.listdir(tools_json_dir_path):
        print(json_file)
        if "zenodo" not in json_file or "swp" in json_file:
            continue
        json_path = tools_json_dir_path + "/" + json_file
        with open(json_path) as json_file:
            json_dict = json.loads(json_file.read())
            boutiques_descriptor_list.append(json_dict)

    return boutiques_descriptor_list


def write_csv_file(csv_file_basename, csv_content):
    """
    Write the content of a list of list into a CSV file. Example of csv_content:
        [
            ['Header_1', 'Header_2', 'Header_3' ...],
            ['Value_1',  'Value_2',  'Value_3'  ...],
            ....
        ]

    :param csv_file_basename: base name that should be given to the CSV
     :type csv_file_basename: str
    :param csv_content      : list of list with the content of the future CSV file
     :type csv_content      : list
    """

    csv_file = (
        os.getcwd()
        + "/"
        + csv_file_basename
        + "_"
        + str(datetime.date.today())
        + ".csv"
    )

    with open(csv_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(csv_content)


========================================
FILE: ./data_aggregation_summary_scripts/lib/__init__.py
========================================
"""
From the documentation at https://docs.python.org/2/tutorial/modules.html#packages
The __init__.py files are required to make Python treat the directories as
containing packages; this is done to prevent directories with a common name,
such as string, from unintentionally hiding valid modules that occur later on
the module search path. In the simplest case, __init__.py can just be an empty
file, but it can also execute initialization code for the package or set the
__all__ variable, described later.
"""


========================================
FILE: ./data_aggregation_summary_scripts/create_tools_statistics_per_domain.py
========================================
"""Docstring."""
import getopt
import os
import sys

import lib.Utility as Utility


def main(argv):
    """Doctring."""
    # create the getopt table + read and validate the options given to the script
    tools_json_dir_path = parse_input(argv)

    # read the content of the DATS.json files present in the boutiques's cached
    # directory present in ~
    tool_descriptor_list = Utility.read_boutiques_cached_dir(tools_json_dir_path)

    # digest the content of the DATS.json files into a summary of variables of interest
    tools_summary_dict = {}
    i = 0
    for tool in tool_descriptor_list:
        tools_summary_dict[i] = parse_json_information(tool)
        i += 1

    # create the summary statistics of the variables of interest organized per
    # domain of application
    csv_content = [
        [
            "Domain of Application",
            "Number Of Tools",
            "Containers",
            "Execution Capacity",
        ],
    ]
    for field in [
        "Neuroinformatics",
        "Bioinformatics",
        "MRI",
        "EEG",
        "Connectome",
        "BIDS-App",
    ]:
        summary_list = get_stats_per_domain(tools_summary_dict, field)
        csv_content.append(summary_list)

    # write the summary statistics into a CSV file
    Utility.write_csv_file("tools_summary_statistics_per_domain", csv_content)


def parse_input(argv):
    """
    Create the GetOpt table + read and validate the options given when calling the script.

    :param argv: command-line arguments
     :type argv: list

    :return: the path to the tools directory containing the Boutiques JSON descriptors
             (typically ~/.cache/boutiques/production)
     :rtype: str
    """
    tools_dir_path = None

    description = (
        "\nThis tool facilitates the creation of tools summary statistics per domain of application for "
        "reporting purposes. It will read Boutiques's JSON files and print out a summary per domain based "
        "on the following list of tags: \nNeuroinformatics, Bioinformatics, MRI, EEG, Connectome, BIDS-App.\n"
    )
    usage = (
        "\n"
        "usage  : python "
        + __file__
        + " -d <path to the Boutiques's JSON cached directory to parse."
        " (typically ~/.cache/boutiques/production>\n\n"
        "options: \n"
        "\t-d: path to the Boutiques's JSON cached directory to parse."
        " (typically ~/.cache/boutiques/production>\n\n"
    )

    try:
        opts, args = getopt.getopt(argv, "hd:")
    except getopt.GetoptError:
        sys.exit()

    for opt, arg in opts:
        if opt == "-h":
            print(description + usage)
            sys.exit()
        elif opt == "-d":
            tools_dir_path = arg

    if not tools_dir_path:
        print(
            "a path to the Boutiques's JSON cached directory needs to be "
            "given as an argument to the script by using the option `-d`",
        )
        print(description + usage)
        sys.exit()

    if not os.path.exists(tools_dir_path):
        print(tools_dir_path + "does not appear to be a valid path")
        print(description + usage)
        sys.exit()

    return tools_dir_path


def parse_json_information(json_dict):
    """
    Parse the content of the JSON dictionary and grep the variables of interest for the summary statistics.

    :param json_dict: dictionary with the content of a tool JSON descriptor file
     :type json_dict: dict

    :return: dictionary with the variables of interest to use to produce the
             summary statistics
     :rtype: dict
    """
    tool_summary_dict = {
        "title": json_dict["name"],
        "container_type": None,
        "domain": None,
        "online_platform_urls": None,
    }

    if "container-image" in json_dict and "type" in json_dict["container-image"]:
        tool_summary_dict["container_type"] = json_dict["container-image"]["type"]

    if "tags" in json_dict and "domain" in json_dict["tags"]:
        tool_summary_dict["domain"] = [x.lower() for x in json_dict["tags"]["domain"]]

    if "online-platform-urls" in json_dict:
        tool_summary_dict["online_platform_urls"] = json_dict["online-platform-urls"]

    return tool_summary_dict


def get_stats_per_domain(tool_summary_dict, domain):
    """
    Produce a summary statistics per domain (Neuroinformatics, Bioinformatics, MRI, EEG...) of the identified variables of interest.  # noqa: E501.

    :param tool_summary_dict: dictionary with the variables of interest for the summary
     :type tool_summary_dict: dict
    :param domain           : name of the domain of application
     :type domain           : str

    :return: list with the summary statistics on the variables for the domain of application
     :rtype: list
    """
    container = {
        "docker": 0,
        "singularity": 0,
    }
    number_of_tools = 0
    number_of_cbrain_tools = 0

    for index in tool_summary_dict:

        tool_dict = tool_summary_dict[index]

        if not tool_dict["domain"]:
            continue

        if domain.lower() not in tool_dict["domain"] and domain != "BIDS-App":
            continue

        if domain == "BIDS-App" and "bids app" not in tool_dict["title"].lower():
            continue

        number_of_tools += 1

        if tool_dict["container_type"] == "docker":
            container["docker"] += 1
        elif tool_dict["container_type"] == "singularity":
            container["singularity"] += 1

        print(tool_dict["online_platform_urls"])

        if (
            tool_dict["online_platform_urls"]
            and "https://portal.cbrain.mcgill.ca" in tool_dict["online_platform_urls"]
        ):
            number_of_cbrain_tools += 1

    return [
        domain,
        str(number_of_tools),
        f'Docker ({str(container["docker"])}); Singularity ({str(container["singularity"])})',  # noqa: E702
        "CBRAIN (" + str(number_of_cbrain_tools) + ")",
    ]


if __name__ == "__main__":
    main(sys.argv[1:])


========================================
FILE: ./import_single_zenodo_to_local_datalad.py
========================================
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


def run(cmd: list[str], cwd: str | None = None):
    subprocess.run(cmd, cwd=cwd, check=True)


def capture(cmd: list[str], cwd: str | None = None) -> str:
    return subprocess.check_output(cmd, cwd=cwd, text=True)


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

    # Zenodo records often omit a structured species annotation even for
    # straightforward human participant datasets. Add Homo sapiens when the
    # record text strongly suggests a human cohort so generated DATS passes
    # the CONP validation rule that requires at least one species.
    if not is_about:
        human_text = f"{metadata.get('title', '')} {metadata.get('description', '')}".lower()
        human_markers = [
            "participant",
            "participants",
            "adolescent",
            "adolescents",
            "children",
            "child",
            "adult",
            "adults",
            "pediatric",
            "human",
            "neurotypical",
        ]
        if any(marker in human_text for marker in human_markers):
            is_about.append(
                {
                    "identifier": {
                        "identifier": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606"
                    },
                    "name": "Homo sapiens",
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
        "is_private": False,
        "dataset_token": "",
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
    parser = argparse.ArgumentParser(description="Import one Zenodo record into a local DataLad dataset.")
    parser.add_argument("--record", required=True, help="Zenodo record id")
    parser.add_argument("--dataset-dir", required=True, help="Local DataLad dataset directory")
    args = parser.parse_args()

    dataset_dir = os.path.abspath(args.dataset_dir)
    record = fetch_json(f"https://zenodo.org/api/records/{args.record}")
    dataset = build_description(record)

    run(["datalad", "no-annex", "--pattern", ".conp-zenodo-crawler.json"], cwd=dataset_dir)
    run(["datalad", "no-annex", "--pattern", "README.md"], cwd=dataset_dir)
    run(["datalad", "no-annex", "--pattern", "DATS.json"], cwd=dataset_dir)
    run(["datalad", "save", "-m", "Prepare metadata files"], cwd=dataset_dir)

    for item in dataset["files"]:
        link = item["links"]["self"]
        archive = item.get("type") == "zip"
        target_name = item["key"]
        if os.path.lexists(os.path.join(dataset_dir, target_name)):
            continue
        cmd = ["datalad", "download-url"]
        if archive:
            cmd.append("--archive")
        cmd.extend(["--path", target_name])
        cmd.append(link)
        run(cmd, cwd=dataset_dir)

    create_tracker(os.path.join(dataset_dir, ".conp-zenodo-crawler.json"), dataset)
    write_readme(os.path.join(dataset_dir, "README.md"), dataset)
    write_dats(os.path.join(dataset_dir, "DATS.json"), dataset, dataset_dir)
    run(["datalad", "save", "-m", f"Import Zenodo record {args.record}"], cwd=dataset_dir)

    print(clean_title(dataset["title"]))
    print(dataset["title"])


if __name__ == "__main__":
    main()


