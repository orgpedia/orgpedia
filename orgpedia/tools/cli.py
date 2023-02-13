import json  # noqa
import shutil
import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pkg_resources
import typer
import yaml

from orgpedia.tools.flow import Flow, Task, get_flow_task_dir

Writeable_Dir = typer.Argument(..., exists=True, file_okay=False, writable=True, resolve_path=False)
Readable_Dir = typer.Argument(..., exists=True, file_okay=False, readable=True, resolve_path=False)

app = typer.Typer()


@app.command()
def extract(package: str, extract_dir: Path = Writeable_Dir, objects: str = 'all'):
    packages = [package]
    if not packages:
        print('No packages to import.')
        return

    for package in packages:
        # https://stackoverflow.com/questions/54597212
        package = package.replace('-', '_')

        package_extract_dir = extract_dir / package

        # when support for objects and stdout is added, this should be removed
        if package_extract_dir.exists():
            print(f'data package exists at {package_extract_dir}, skipping')
            continue

        try:
            zip_path = Path(pkg_resources.resource_filename(package, 'data.zip'))
        except ModuleNotFoundError:
            print(f"Error: Unable to locate: '{package}'")
            raise typer.Abort()

        if not zip_path.exists():
            print(f"Error: Unable to locate data.zip dir: in '{package}'")
            raise typer.Abort()

        if objects == 'all':
            with ZipFile(zip_path) as zip_file:
                zip_file.extractall(package_extract_dir)


@app.command()
def importAll(import_dir: Path = Writeable_Dir):
    importPackages(import_dir / 'data_packages')
    importModels(import_dir / 'models')


@app.command()
def importPackages(packages_dir: Path = Writeable_Dir):
    packages_file = packages_dir / 'data_packages.yml'
    if not packages_file.exists():
        print(f'No data-packages to import. As data_packages.yml is missing: {packages_file}')
        return

    packages_dict = yaml.load(packages_file.read_text(), Loader=yaml.FullLoader)
    if not packages_dict:
        print(f'No data-packages to import. As data_packages.yml is empty: {packages_file}')
        return

    for (name, info) in packages_dict.items():
        print(f'data-package: {name}')
        extract(name, packages_dir)


@app.command()
def importModels(models_dir: Path = Writeable_Dir, models: str = 'all'):
    models_file = models_dir / 'models.yml'

    if not models_file.exists():
        print(f'No models to import. As models.yml is missing: {models_file}')
        return

    print('This commands takes a very long time (hours), check help for options.\n')
    models_dict = yaml.load(models_file.read_text(), Loader=yaml.FullLoader)

    for (name, info) in models_dict.items():
        source, stub = name.split(':')

        if source == 'local':
            continue

        print(f'Model: {name}')

        model_dir = models_dir / source / stub
        if model_dir.exists():
            print(f'\tSkipping {name} as {model_dir} exists\n')
            continue

        git_cmd = ['git']
        if 'branch' in info:
            git_cmd += ['--branch', info['branch'], '--single-branch']
        git_cmd += ['clone', info['git_url'], str(model_dir)]

        print(f'\tCloning: {" ".join(git_cmd)}')
        subprocess.check_call(git_cmd)

        if 'commit_sha' in info:
            git_cmd = ['cd', str(model_dir), '&&', 'git', 'reset', '--hard']
            git_cmd += [info['commit_sha']]
            print(f'\tReseting: {" ".join(git_cmd)}')
            subprocess.check_call(git_cmd)
        print()


@app.command()
def exportPackage(data_dir: Path = Readable_Dir, export_dir: Path = Writeable_Dir):
    """Export orders and docs from the final task directory."""

    data_dir = Path(data_dir)
    export_dir = Path(export_dir)

    (export_dir / '__init__.py').touch()

    with ZipFile(export_dir / 'data.zip', 'w', ZIP_DEFLATED) as data_zip:
        for data_path in data_dir.glob('**/*'):
            zip_path = 'data' / data_path.relative_to(data_dir)
            data_zip.write(data_path, zip_path)
        # end for


@app.command()
def exportSite(task_dir: Path = Readable_Dir, export_dir: Path = Writeable_Dir):
    output_dir = task_dir / "output"

    shutil.rmtree(export_dir)
    shutil.copytree(output_dir, export_dir, symlinks=False)  # copy contents of symlink


@app.command()
def check():
    flow_dir, task_dir = get_flow_task_dir()
    if task_dir:
        task = Task(task_dir, flow_dir)
        task.check_files()
    elif flow_dir:
        flow = Flow(flow_dir)
        flow.check_files()
    else:
        print('Unable to locate flow or task directory')
        raise typer.Abort()


@app.command()
def readme():
    flow_dir, task_dir = get_flow_task_dir()
    if task_dir:
        task = Task(task_dir, flow_dir)
        readme_path = task_dir / 'README.md'
        readme_path.write_text(task.show_readme())
    elif flow_dir:
        flow = Flow(flow_dir)
        for task in flow.tasks:
            task_readme_path = task.taskDir / 'README.md'
            task_readme_path.write_text(task.show_readme())
        readme_path = flow_dir / 'README.md'
        readme_path.write_text(flow.show_readme())
    else:
        print('Unable to locate flow or task directory')
        raise typer.Abort()


"""
@app.command()
def check(name: str):
    print(f"Hello {name}")

@app.command()
def checkSite(name: str):
    print(f"Hello {name}")

@app.command()
def genReadme(name: str):
    print(f"Hello {name}")

@app.command()
def linkUpstream(name: str):
    print(f"Hello {name}")

@app.command()
def createTask(name: str):
    print('hello')

@app.command()
def diffOutput(name: str):
    print(f"Hello {name}")

@app.command()
def stats(name: str):
    print(f"Hello {name}")

@app.command()
def check(name: str, formal: bool = False):
    if formal:
        print(f"Goodbye Ms. {name}. Have a good day.")
    else:
        print(f"Bye {name}!")

"""


def main():
    return app()


if __name__ == "__main__":
    app()
