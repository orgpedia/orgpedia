import typer
import json
import pkg_resources
from zipfile import ZipFile
from pathlib import Path
import shutil

Writeable_Dir = typer.Argument(..., exists=True, file_okay=False, writable=True, resolve_path=True)
Readable_Dir = typer.Argument(..., exists=True, file_okay=False, readable=True, resolve_path=True)

import data_package

app = typer.Typer()

"""
@app.command()
def extract(module: str, extract_dir: Writeable_Dir, objects:str='all'):
    try:
        data_path = Path(pkg_resources.resource_filename(module, 'data'))
    except ModuleNotFoundError:
        print(f"Error: Unable to locate '{module}'")
        typer.abort()

    if not data_path.exists():
        print(f"Error: Unable to locate data dir in '{module}'")
        typer.abort()

    if objects == 'all':
        data_package.extract_all(data_path, extract_dir)
    else:
        object_type, *object_names = objects.split(':', 1)

        object_types = ('docs', 'orders', 'officer_info', 'post_info')
        if object_type not in object_types:
            print(f"Error: incorrect object_type '{object}' choose from {object_types}")
            typer.abort()

        object_names = object_names[0].split(',') if object_names else []
        proc = getattr(data_package, f'extract_{object_type}')
        proc(extract_dir, object_names)


"""

@app.command()
def export(task_dir: Path=Readable_Dir, export_dir: Path=Writeable_Dir):
    def write_zip(zip_path, file_path):
        with ZipFile(zip_path, 'w') as zip_file:
            zip_file.write(file_path)
    
    # if not is_readable(task_dir / 'output'):
    #     print("Unable to find the 'output' directory in {task_dir}")
    #     typer.abort()

    task_dir = Path(task_dir)
    export_dir = Path(export_dir)

    (export_dir / '__init__.py').touch()
    data_dir = export_dir / 'data'
    output_dir = task_dir / 'output'
    
    data_dir.mkdir(exist_ok=True)
    with ZipFile(data_dir / 'docs.zip', 'w') as docs_zip, ZipFile(data_dir / 'orders.zip', 'w') as orders_zip:
        #docs_zip.mkdir('docs')
        #orders_zip.mkdir('orders')
        docs_zip_path, orders_zip_path = Path('docs'), Path('orders')
        
        for order_path in output_dir.glob('*.order.json'):
            doc_zip_path = docs_zip_path / order_path.name.replace('.order.', '.doc.')
            docs_zip.write(order_path, doc_zip_path)

            doc_json = json.loads(order_path.read_text())
            order_json = doc_json['order']
            
            order_zip_path = orders_zip_path / order_path.name
            orders_zip.write(order_path, order_zip_path)
        #end for

    write_zip(data_dir / 'tenures.zip', output_dir / 'tenures.json')
    #write_zip(data_dir / 'officer_infos.zip', output_dir / 'officer_infos.json')
    #write_zip(data_dir / 'post_infos.zip', output_dir / 'post_infos.json')        

@app.command()
def exportSite(task_dir: Path=Readable_Dir, export_dir: Path=Writeable_Dir):
    output_dir = task_dir / "output"
    
    shutil.rmtree(export_dir)
    shutil.copytree(output_dir, export_dir, symlinks=False) # copy contents of symlink

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

if __name__ == "__main__":
    app()

