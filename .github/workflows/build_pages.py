import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any, cast

import namesgenerator
import tomlkit

BUILD_OUTPUT_FOLDER = os.environ["BUILD_OUTPUT_FOLDER"]
project_root = Path(__file__).parent.parent.parent
model_dir_name = "gallery/statistical_rethinking"
mcmc_dir_name = "gallery/mcmc"
juliabugs_dir_name = "gallery/juliabugs"
SHA = os.environ["COMMIT_SHA"]


def create_workflow(model_dir: Path, model_parent_dir_name: str):
    template_dir = f"{project_root}/workflow/Coinfer.py/Coinfer/invoke_cmd"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir) / "workflow"
        temp_dir.mkdir(parents=True)

        shutil.copytree(model_dir, temp_dir / "model")
        new_project_toml = _patch_our_dependencies(
            Path(temp_dir, "model", "Project.toml").read_text()
        )
        Path(temp_dir, "model", "Project.toml").write_bytes(new_project_toml)

        shutil.copytree(project_root / "workflow", temp_dir / "client")
        shutil.copy(f"{template_dir}/README.md.template", temp_dir / "README.md")
        Path(temp_dir, "workflow.yaml").write_text(
            Path(f"{template_dir}/workflow.yaml.template")
            .read_text()
            .format(model_name=namesgenerator.get_random_name())
        )

        shutil.copy(model_dir / "data.py", temp_dir / "data.py")
        data_file = model_dir / "data.csv"
        if data_file.is_file():
            shutil.copy(data_file, temp_dir / "data")

        model_script = model_dir / "script.jl"
        if model_script.is_file():
            shutil.copy(model_script, temp_dir / "model" / "script.jl")
        else:
            Path(temp_dir, "model", "script.jl").write_text(
                Path(f"{template_dir}/startup_script.jl.template").read_text()
            )
        Path(temp_dir, "model", ".metadata").write_text('{"entrance_file": "model.jl"}')

        shutil.copy(f"{template_dir}/tasks.py", Path(temp_dir, "tasks.py"))
        Path(temp_dir, "pyproject.toml").write_text(
            Path(f"{template_dir}/pyproject.toml.template")
            .read_text()
            .format(project_name=model_dir.name)
        )

        shutil.make_archive(str(model_dir), "zip", temp_dir.parent, "workflow")
        shutil.copy(
            str(model_dir) + ".zip", project_root / BUILD_OUTPUT_FOLDER / model_parent_dir_name
        )


def _patch_our_dependencies(project_toml: str):
    extra_deps: dict[str, str] = {
        "AbstractMCMC": "80f14c24-f653-4e6a-9b94-39d6b0f70001",
        "StableRNGs": "860ef19b-820b-49d6-a774-d7a799459cd3",
    }

    extra_sources: dict[str, str] = {}

    data = tomlkit.parse(project_toml)

    if "deps" not in data:
        data["deps"] = tomlkit.table()
    for key, val in extra_deps.items():
        deps = cast(dict[str, Any], data["deps"])
        if key not in deps:
            deps[key] = val

    if "sources" not in data:
        data["sources"] = tomlkit.table()
    for key, val in extra_sources.items():
        sources = cast(dict[str, Any], data["sources"])
        if key not in sources:
            sources[key] = val

    return tomlkit.dumps(data).encode("utf-8")  # type: ignore


def create_workflow_from_models(
    pages_root_url: str, output_dir: Path, parent_dir_name: str
) -> list[dict[str, str]]:
    models: list[dict[str, str]] = []

    os.makedirs(output_dir / parent_dir_name, exist_ok=True)
    for file in (project_root / parent_dir_name).glob("*"):
        if not file.is_dir():
            continue

        create_workflow(file, parent_dir_name)
        model_name = file.name
        url = f"{pages_root_url}/{parent_dir_name}/{model_name}.zip"
        models.append({"name": model_name, "url": url})
    return models


def generate_files():
    root = os.environ["PAGES_ROOT_URL"]
    output_dir = project_root / BUILD_OUTPUT_FOLDER

    output_dir.mkdir(parents=True, exist_ok=True)

    models = create_workflow_from_models(root, output_dir, model_dir_name)
    juliabugs_models = create_workflow_from_models(root, output_dir, juliabugs_dir_name)

    workflow_dir = output_dir / "workflow"
    workflow_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        shutil.copytree(project_root / "workflow", temp_dir / "client")
        shutil.make_archive(
            str(workflow_dir / f"coinfer-{SHA[:7]}"),
            "zip",
            temp_dir,
            "client",
        )
        shutil.copy(
            str(workflow_dir / f"coinfer-{SHA[:7]}") + ".zip",
            workflow_dir / "coinfer-latest.zip",
        )

    for file in (project_root / mcmc_dir_name).rglob("*"):
        if file.is_dir():
            continue
        rel_file = file.relative_to(project_root)
        os.makedirs(output_dir / rel_file.parent, exist_ok=True)
        with zipfile.ZipFile(
            str(output_dir / (str(rel_file) + ".zip")), "w", zipfile.ZIP_DEFLATED
        ) as zipf:
            zipf.write(file, rel_file)

    d: Any = {
        "models": {
            "StatisticalRethinking": models,
            "JuliaBUGS": juliabugs_models,
        },
        "MCMC": {
            "Arviz": [
                {
                    "name": data_file.stem,
                    "url": [f"{root}/{mcmc_dir_name}/Arviz/{data_file.name}.zip"],
                }
                for data_file in Path(project_root / mcmc_dir_name / "Arviz").glob(
                    "*.nc"
                )
            ],
            "Stan": [
                {
                    "name": "finite-mixture",
                    "url": [
                        f"{root}/{mcmc_dir_name}/Stan/{data_file.name}.zip"
                        for data_file in Path(
                            project_root / mcmc_dir_name / "Stan"
                        ).glob("finite-mixture-20250806105328_*.csv")
                    ],
                }
            ],
            "Turing": [
                {
                    "name": "demo",
                    "url": [f"{root}/{mcmc_dir_name}/Turing/demo_chain.csv.zip"],
                }
            ],
        },
    }

    with open(output_dir / "example-models.json", "w") as f:
        json.dump(d, f, indent=2)


def main():
    generate_files()
    subprocess.run(
        f"tree -A {BUILD_OUTPUT_FOLDER}", shell=True, check=True, cwd=project_root
    )


if __name__ == "__main__":
    main()
