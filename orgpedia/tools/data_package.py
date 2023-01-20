from pathlib import Path
from zipfile import ZipFile
import pkg_resources

def get_path(file_name):
    #return Path(pkg_resources.resource_filename('orgpedia_mahapol', file_name))
    return Path('data') / file_name

def extract_from_zip(zip_path, output_path, files):
    files = files if isinstance(files, list) else [files]
    def get_zip_file(file):
        zip_files = [f for f in namelist if f.name == file]
        return zip_files[0]

    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise ValueError(f'Unable to locate {zip_path}')


    with ZipFile(zip_path) as zip_file:
        namelist = [Path(f) for f in zip_file.namelist() if not f.endswith('/')]
        file_zip_names = [ (f, get_zip_file(f)) for f in files ]
        
        missing = [ f for (f, z) in file_zip_names if not z]
        if missing:
            missing_str = ",".join(missing)
            raise ValueError('Unable to locate {missing_str} in zip: {zip_path}')

        files = namelist if not files else [z for (f, z) in file_zip_names]
        for f in files:
            zip_file.extract(str(f), output_path)

def extract_docs(path, docs=[]):
    docs_zip_path = get_path('docs.zip')
    extract_from_zip(docs_zip_path, path, docs)
                             

def extract_orders(path, orders=[]):
    orders_zip_path = get_path('orders.zip')
    extract_from_zip(orders_zip_path, path, orders)


def extract_officer_infos(path):
    officer_infos_zip_path = get_path('officer_infos.json.zip')    
    extract_from_zip(officer_infos_zip_path, path, ['officer_infos.json'])


def extract_post_infos(path):
    post_infos_zip_path = get_path('post_infos.json.zip')        
    extract_from_zip(post_infos_zip_path, path, ['post_infos.json'])

def extract_all(path):
    extract_docs(path)
    extract_orders(path)
    extract_officer_infos(path)
    extract_post_infos(path)    


if __name__ == '__main__':
    extract_docs('/tmp/mahapol', '1_Upload_10.pdf.doc.json')
    extract_orders('/tmp/mahapol', '1_Upload_10.pdf.order.json')    

    extract_all('/tmp/mahapol')
