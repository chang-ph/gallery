import json
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any

project_root = Path(__file__).parent.parent.parent
output_dir = project_root / "build_outputs_folder"
output_dir.mkdir(parents=True, exist_ok=True)


root = os.environ['PAGES_ROOT_URL']
models: list[dict[str, str]] = []

os.makedirs(output_dir / "StatisticalRethinkingJulia", exist_ok=True)
for file in (project_root / "StatisticalRethinkingJulia").glob("*"):
    if not file.is_dir():
        continue

    model_name = file.name
    shutil.make_archive(str(output_dir / "StatisticalRethinkingJulia" / model_name), "zip", file)

    url = f"{root}/StatisticalRethinkingJulia/{model_name}.zip"
    models.append({"name": model_name, "url": url})

for file in (project_root / "MCMC").rglob("*"):
    if file.is_dir():
        continue
    rel_file = file.relative_to(project_root)
    os.makedirs(output_dir / rel_file.parent, exist_ok=True)
    print(file, rel_file)
    with zipfile.ZipFile(str(output_dir / (str(rel_file) + ".zip")), "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file, rel_file)

d: Any = {
    "models": {
        "StatisticalRethinking": models,
    },
    "MCMC": {
        "Arviz": [
            {
                "name": data_file.stem,
                "url": [f"{root}/MCMC/Arviz/{data_file.name}.zip"],
            } for data_file in Path(project_root / "MCMC/Arviz").glob("*.nc")
        ],
        "Stan": [
            {
                "name": "finite-mixture",
                "url": [
                    f"{root}/MCMC/Stan/{data_file.name}.zip"
                    for data_file in Path(project_root / "MCMC/Stan").glob("finite-mixture-20250806105328_*.csv")
                ],
            }
        ],
        "Turing": [
            {"name": "demo", "url": [f"{root}/MCMC/Turing/demo_chain.csv.zip"]}
        ],
    },
}

with open(output_dir / "example-models.json", "w") as f:
    json.dump(d, f, indent=2)