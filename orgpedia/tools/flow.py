import os
import json
import pathlib
import sys
from collections import Counter

# import graphviz
import yaml
from more_itertools import first, pairwise


def get_all_exts(path):
    if '.' in path.name:
        exts = path.name.split('.')[1:]

        return '.'.join(exts)
    else:
        return ''


def get_link_src(target):
    assert target.is_symlink(), f'{target} not a symlink'
    src = pathlib.Path(os.readlink(target))
    if '..' in src.parts:
        targetParts = list(target.parent.parts)
        for part in src.parts:
            if part == '..':
                targetParts.pop()
            else:
                targetParts.append(part)
        return pathlib.Path(*targetParts)
    else:
        return src


def get_pdf_file(file_path):
    if isinstance(file_path, pathlib.Path):
        file_name = file_path.name
    else:
        file_name = file_path

    if '.pdf' in file_name:
        return file_name[: file_name.index('.pdf') + 4]
    else:
        return ''


def get_pdf_files(file_paths):
    pdf_files = [get_pdf_file(f) for f in file_paths]
    pdf_files = [pathlib.Path(f) for f in pdf_files if f]
    return pdf_files


def get_link_src_subdir(link_files, parent_dir):
    num_dirs = len(parent_dir.parents)
    srcs = [get_link_src(f) for f in link_files if f.is_symlink()]
    mat_srcs = [s for s in srcs if parent_dir in s.parents]
    mat_subdirs = set(s.parents[len(s.parents) - (num_dirs + 2)] for s in mat_srcs)
    return mat_subdirs


class SubTask:
    def __init__(self, yml_dict, task, sub_task_file):
        self.task = task
        self.name = yml_dict['name']
        self.sub_task_file = sub_task_file
        self.stub = ''
        for stub in ['stub', 'conf_stub']:
            if stub in yml_dict:
                self.stub = yml_dict[stub]
                break
            elif stub in yml_dict['config']:
                self.stub = yml_dict['config'][stub]
        self.stub = self.name if not self.stub else self.stub

    @property
    def full_name(self):
        return f"{self.task.name}->{self.name}"

    def num_conf(self):
        cnf_files = self.task._getTaskFiles('conf')
        sub_cnf_files = [c for c in cnf_files if self.stub in c.name]
        print(f'{self.name} -> stub: {self.stub} {len(sub_cnf_files)}')
        return len(sub_cnf_files)

class Task:
    def __init__(self, taskDir, flowDir):

        self.taskDir = taskDir
        self.flowDir = flowDir
        self.name = self._getTaskName(self.taskDir)

        self.iptFiles = self._getTaskFiles('input')
        self.optFiles = self._getTaskFiles('output')

        self.upstream = self._getUpTasks()
        self.downstreamTasks = []

        self.skipped_docs = {}
        self.sub_tasks = self._getSubTasks()

        importDir = flowDir.parent / 'import'
        i_paths = get_link_src_subdir(self.iptFiles, importDir)
        self.importSubDirs = [i.relative_to(self.flowDir.parent) for i in i_paths]

        #exportDir = flowDir.parent / 'export'        
        #e_paths = get_link_src_subdir(self.iptFiles, exportDir)
        
        self.exportSubDirs = []

        self.errors = {}
        self.edits = {}
        
        #self.calculate_error_edits() 

    def calculate_error_edits(self):
        output_ext, _ = self.get_output_ext_counts()
        doc_files = [p for p in self.optFiles if output_ext in p.name ]
        
        for doc_file in doc_files:
            json_doc = json.loads(doc_file.read_text())
            for sub_task in self.sub_tasks:
                name = sub_task.name
                self.errors.setdefault(name, []).extend(json_doc.get('errors', {}).get(name, []))
                self.edits.setdefault(name, []).extend(json_doc.get('edits', {}).get(name, []))
        #end for

    def get_ext_counts(self, file_paths):
        def pdf_suffixes(pdf_file):
            suffixes = pdf_file.suffixes
            assert '.pdf' in suffixes, f'not a pdf file {pdf_file}'
            pdf_idx = suffixes.index('.pdf')
            return suffixes[pdf_idx:]

        return Counter([''.join(pdf_suffixes(f)) for f in file_paths if '.pdf' in f.suffixes])

    def get_input_ext_counts(self):
        ext_counts = self.get_ext_counts(self.iptFiles)
        if not ext_counts:
            return '', 0
        max_ext = max(ext_counts, key=ext_counts.get)
        return max_ext, ext_counts[max_ext]

    def get_output_ext_counts(self):
        ext_counts = self.get_ext_counts(self.optFiles)
        max_ext = max(ext_counts, key=ext_counts.get, default=0)
        if self.downstreamTasks:
            downstream_ext, _ = self.downstreamTasks[0].get_input_ext_counts()
            max_ext = downstream_ext if downstream_ext in ext_counts else max_ext
        return max_ext, ext_counts[max_ext]

    def get_intermediates_ext_counts(self):
        ext_counts = self.get_ext_counts(self.optFiles)
        if len(ext_counts) == 1:
            return []
        opt_ext, _ = self.get_output_ext_counts()
        return list((e, c) for (e, c) in ext_counts.items() if e != opt_ext)

    def get_skipped_docs(self):
        # returns string that can be printed on README'
        result = []
        for ipt_file, skipped_info in self.skipped_docs.items():
            if isinstance(skipped_info, list):
                reason = f'Skipped in {ipt_file}'
                result += [(s, reason) for s in skipped_info]
            elif isinstance(skipped_info, dict):
                result += [(s, f'{ipt_file} {r}') for s, r in skipped_info.items()]
            else:
                raise NotImplementedError(f'Unknown type {type(skipped_info)}')
        return result

    def get_skipped_pdfs(self):
        # returns only a list of pdf files that are skipped'        
        result = []
        for ipt_file, skipped_info in self.skipped_docs.items():
            if isinstance(skipped_info, list):
                result += skipped_info
            elif isinstance(skipped_info, dict):
                result += [s for s, r in skipped_info.items()]
            else:
                raise NotImplementedError(f'Unknown type {type(skipped_info)}')
        return get_pdf_files(result)

    @property
    def sub_task_full_names(self):
        return [st.full_name for st in self.sub_tasks]

    def addDwnTask(self, task):
        if id(task) not in [id(t) for t in self.downstreamTasks]:
            self.downstreamTasks.append(task)

    # todo, move this to flow dir
    def get_total_errors(self):
        return sum(st.get_total_errors() for st in self.sub_tasks)
    
    # todo, move this to flow dir
    def get_error_summary(self):
        return ', '.join(f'{st.name}: {st.get_total_errors()}' for st in self.sub_tasks if st.get_total_errors())

    def _getTaskName(self, dirPath):
        dirPathStr = str(dirPath)
        flowPathStr = str(self.flowDir)
        if flowPathStr in dirPathStr and dirPath.name not in ['hand']:
            taskName = dirPathStr[len(flowPathStr) + 1 :]
            taskName = taskName[: taskName.rindex("_") + 1]
            return taskName
        else:
            return ''

    def _getTaskFiles(self, subdir_name):
        if subdir_name == 'input':
            subDir = self.taskDir / subdir_name
            linkFiles = subDir.glob("*")
            linkFiles = [l for l in linkFiles if l.is_symlink() and not l.name.startswith('.')]
            return linkFiles
        elif subdir_name == 'output':
            subDir = self.taskDir / subdir_name
            optFiles = list(subDir.glob("*.json")) + list(subDir.glob("*.html"))
            return optFiles

        elif subdir_name == 'conf':
            subDir = self.taskDir / subdir_name
            cnfFiles = list(subDir.glob("*.yml"))
            return cnfFiles
        elif subdir_name == 'logs':
            subDir = self.taskDir / subdir_name
            logFiles = list(subDir.glob("*.logs"))
            return logFiles
        else:
            raise NotImplementedError('Cannot list files in {subdir_name}')

    def _getUpTasks(self):
        srcIptFiles = [get_link_src(l) for l in self.iptFiles]
        srcParentDirs = set([s.parent for s in srcIptFiles])
        taskNames = set([self._getTaskName(pDir) for pDir in srcParentDirs])
        taskNames = [tName for tName in taskNames if tName]
        return taskNames

    def _getSubTasks(self):
        def read_sub_tasks(sub_task_file):
            sub_task_files = [sub_task_file]
            if sub_task_file.name == '.info.yml':
                sub_task_names = sub_task_file.read_text().split('\n')
                sub_task_files = [sub_task_file.parent / n for n in sub_task_names if n]

            self.sub_task_files = sub_task_files
            all_sub_tasks = []
            for st_file in sub_task_files:
                yml_dict = yaml.load(st_file.read_text(), Loader=yaml.FullLoader)
                sub_dicts = yml_dict['pipeline']
                sub_tasks = [SubTask(yt, self, st_file) for yt in sub_dicts]
                sub_tasks = [st for st in sub_tasks if st.name != 'html_generator']
                self.skipped_docs[st_file.name] = yml_dict.get('ignore_docs', [])  # TODO
                all_sub_tasks.extend(sub_tasks)
            return all_sub_tasks

        src_dir = self.taskDir / 'src'
        yml_files = list(src_dir.glob('*.yml'))
        info_file_path = src_dir / '.info.yml'

        if len(yml_files) == 0:
            self.sub_task_files = []
            return []
        if len(yml_files) == 1:
            return read_sub_tasks(yml_files[0])
        elif info_file_path in yml_files:
            return read_sub_tasks(info_file_path)
        else:
            raise ValueError(f'Unable to find .info.yml in {str(src_dir)}')

    def show(self):
        results = [f'Task:{self.name}']
        for sub_task in self.sub_tasks:
            total_entities = sub_task.get_total_entities()
            error_percent = sub_task.get_error_percent()

            if error_percent is not None and total_entities is not None:
                results.append(f'\tSub-task: {sub_task.name} error: {total_entities}[{error_percent:.2f}%]')
            else:
                results.append(f'\tSub-task: {sub_task.name}')
        print('\n'.join(results))

    # def show_mermaid(self):
    #     s = '```mermaid\ngraph TD;\n'
    #     for (st1, st2) in pairwise(self.sub_tasks):
    #         total_entities = st1.get_total_entities()
    #         error_percent = 90 #st1.get_error_percent()
    #         total_str = total_entities if total_entities else ''
    #         error_str = f'{error_percent:.2f}%' if error_percent else ''

    #         t1_l1 = f'<div align=left>{st1.name}<div/><br/>'
    #         t1_l2 = f'Total: {total_str}[{error_str}]'
    #         s += f'\t{st1.name} [{t1_l1}{t1_l2}] --> {st2.name};\n'
    #     s += '```\n'

    #     print(s)

    def show_counts(self):
        ipt_ext, ipt_count = self.get_input_ext_counts()
        opt_ext, opt_count = self.get_output_ext_counts()
        intermediates = self.get_intermediates_ext_counts()

        s =   '| Directory    | Files                          | Counts |\n'
        s += f'|--------------|--------------------------------|--------|\n'
        s += f'| input        | {ipt_ext:30} | {ipt_count:>6} |\n'
        s += f'| output       | {opt_ext:30} | {opt_count:>6} |\n'

        for i_ext, i_count in intermediates:
            s += f'| intermediate | {i_ext:30} | {i_count:>6} |\n'


        sub_tasks = [s for s in self.sub_tasks if s.num_conf()]
        conf_count = sum(s.num_conf() for s in sub_tasks)
        conf_exts = ', '.join([f'{s.stub}[{s.num_conf()}]' for s in sub_tasks])
        s += f'| conf         | {conf_exts:30} | {conf_count:>6} |\n'
        return s

    def show_skipped_docs(self):
        s = '## Skipped Docs\n'
        if not self.skipped_docs:
            s += 'No docs skipped.'
            return s

        image_dir = '/import/images'
        for idx, (doc_name, reason) in enumerate(self.get_skipped_docs()):
            doc_dir = doc_name.replace('.pdf', '')
            s += f'{idx}. [{doc_name}]({image_dir}/{doc_dir}): {reason}\n'
        return s

    def show_sub_tasks(self):
        s = '## Sub Tasks\n'
        s += f'There are {len(self.sub_tasks)} sub_tasks.\n'
        for sub_task in self.sub_tasks:
            errors, edits = self.errors.get(sub_task.name, []), self.edits.get(sub_task.name, [])
            err_str = "" #",".join(f"{k}: {s}" for (k,v) in Counter(e['name'] for e in errors).items())
            edt_str = ", ".join(f"{k}: {v}" for (k,v) in Counter(e['cmd'] for e in edits).items())            
            
            s += f'\n### {sub_task.name}\n'
            s += f'    Errors: {len(errors)} [{err_str}]\n'
            s += f'    Edits: {len(edits)} [{edt_str}]\n'
            s += f'    Conf Files: {sub_task.num_conf()}\n'

        return s

    def show_footer(self):
        s = '---'
        sub_task_files = ', '.join(str(p) for p in self.sub_task_files)
        s += '* This file is auto-generated from {sub_task_files} *'
        return s

    def show_readme(self):
        s = f'# {task.name}\n\n'
        s += self.show_counts() + '\n'
        s += self.show_skipped_docs() + '\n'
        s += self.show_sub_tasks() + '\n'
        s += self.show_footer() + '\n'        
        return s

    # def checkFlowFlowCount(self):
    #     def is_multiple(x, m):
    #         return x and (m % x) == 0

    #     tskOutput = len(self.optFiles)
    #     dwnInput = sum([len(dTask.iptFiles) for dTask in self.downstreamTasks])

    #     if not is_multiple(tskOutput, dwnInput):
    #         print(f'Mismatch: {self.name} output: {tskOutput} dwnInput: {dwnInput}')

    def check_files(self):
        def is_empty(yml_file):
            assert yml_file.name.endswith('yml')
            yml_dict = yaml.load(yml_file.read_text(), Loader=yaml.FullLoader)
            return True if not yml_dict else False
        
        # opt_ext_counts = self.get_ext_counts(self.optFiles)

        ipt_files = set(get_pdf_files(self.iptFiles))

        for dir_name in ['output', 'conf', 'logs']:
            dir_files = [pathlib.Path(f) for f in self._getTaskFiles(dir_name)]

            if dir_name in ('conf', 'output'):
                zero_files = [f for f in dir_files if f.stat().st_size == 0]
                if zero_files:
                    print(f'Task {self.name} {dir_name} zero_size: {" ".join(z.name for z in zero_files)}')

            if dir_name == 'conf':
                yml_files = [f for f in dir_files if f.suffix.endswith('yml')]
                empty_files = [ f for f in yml_files if is_empty(f) ]
                if empty_files:
                    print(f'Task {self.name} {dir_name} empty: {" ".join(e.name for e in empty_files)}')
                
            dir_files = [f for f in dir_files if '.pdf' in f.suffixes]

            sfx_dict = {}
            for f in dir_files:
                sfx_dict.setdefault("".join(f.suffixes), []).append(f)

            sfx_dict = dict((k, set(get_pdf_files(v))) for (k, v) in sfx_dict.items())
            for (sfx, sfx_files) in sfx_dict.items():
                sfx_extra = [f'{str(p).replace(".pdf", "")}{sfx}' for p in sfx_files - ipt_files]
                sfx_len = len(sfx_extra)
                if sfx_extra:
                    sfx_str = f'[{sfx_len}] {" ".join(sfx_extra)}'
                    print(f'Task {self.name} {dir_name} ext: {sfx} {sfx_str}')  # {",".join(sfx_extra)}')

                if dir_name in ('output', 'logs'):
                    opt_files = sfx_files.union(self.get_skipped_pdfs())
                    ipt_extra = ipt_files - opt_files
                    if ipt_extra:
                        ipt_extra = [f'{str(p)}.{sfx}' for p in ipt_extra]
                        ipt_str = f'[{len(ipt_extra)}] {" ".join(ipt_extra)}'
                        print(f'Task {self.name} {dir_name} ext: {sfx} {ipt_str}')

    def checkTaskFlowCount(self):
        if len(self.iptFiles) == len(self.optFiles):
            return True

        elif self.iptFiles and not self.optFiles:
            print(f'Task Flow Mismatch: {self.name} ipt:{len(self.iptFiles)} opt:{len(self.optFiles)}')
            return False
        else:
            ipt_exts = [get_all_exts(f) for f in self.iptFiles if get_all_exts(f)]
            opt_exts = [get_all_exts(f) for f in self.optFiles if get_all_exts(f)]

            ipt_exts_ctr = Counter([ext for ext in ipt_exts])
            opt_exts_ctr = Counter([ext for ext in opt_exts])

            ipt_flow = len(ipt_exts) / len(ipt_exts_ctr)
            opt_flow = len(opt_exts) / len(opt_exts_ctr)

            if ipt_flow != opt_flow:
                print(
                    f'Task Flow Mismatch: {self.name}:\n\tipt[{len(self.iptFiles)}]:{ipt_exts_ctr}\n\topt:{opt_exts_ctr}'
                )
                return False
            else:
                return True

    def __str__(self):
        s = f'taskDir   : {self.taskDir}\n'
        s += f'name     : {self.name}\n'
        s += f'upstream : {self.upstream}\n'
        s += f'iptCount : {len(self.iptFiles)}\n'
        s += f'optCount : {len(self.optFiles)}\n'
        return s

    def getPaths(self, pdfFile):
        def hasStub(fileNames, pdfFile):
            return any([pdfFile in str(fName) for fName in fileNames])

        paths = []
        print(f'Exploring {self.name} [{pdfFile}]')
        if hasStub(self.iptFiles, pdfFile) and hasStub(self.optFiles, pdfFile):
            if not self.downstreamTasks:
                print(f'\tNewP: {self.name} [{pdfFile}] {len(paths)}')
                return [[self]]
            else:
                for dTask in self.downstreamTasks:
                    dPaths = dTask.getPaths(pdfFile)
                    for path in dPaths:
                        path.append(self)
                        paths.append(path)
                print(f'\tDwnP: {self.name} [{pdfFile}] {len(paths)}')
                return paths
        print(f'\tNone: {self.name}')
        return paths


class Flow:
    def __init__(self, flowDir):
        self.flowDir = flowDir
        self.org_code = flowDir.parent.name
        self.tasks = self._buildTasks()
        self.tasksDict = dict([(t.name, t) for t in self.tasks])
        self.populateDownstream()

        self.headTasks = [t for t in self.tasks if not t.upstream]
        self.tailTasks = [t for t in self.tasks if not t.downstreamTasks]
        self.populateExportDir()

    def _buildTasks(self):
        def isTask(taskDir):
            if '.bak' in taskDir.parts or 'subFlows' in taskDir.parts:
                return False
            subDirs = taskDir.glob("*")
            matchedDirs = [p for p in subDirs if p.name in ("input", "output")]
            return True if len(matchedDirs) == 2 else False

        tasks = []
        for p in self.flowDir.glob("*/**"):
            if p.is_dir() and p.name.endswith("_") and isTask(p):
                print(f'Building Task: {p}')
                tasks.append(Task(p, self.flowDir))
        return tasks

    def populateDownstream(self):
        for task in self.tasks:
            [self.tasksDict[uptaskName].addDwnTask(task) for uptaskName in task.upstream]

    def populateExportDir(self):
        def getTaskName(dir_path):
            rel_path = dir_path.parent.relative_to(self.flowDir)
            return str(rel_path)

        export_dir = self.flowDir.parent / 'export' 
        exp_files = (f for f in export_dir.glob('**/*') if (not f.is_dir()) and f.is_symlink())
        exp_files = list(exp_files)

        src_dirs_exp_dirs = set((get_link_src(f).parent, f.parent) for f in exp_files)
        task_names_exp_dirs = set((getTaskName(s), e) for s, e in src_dirs_exp_dirs)
        for task_name, export_dir in task_names_exp_dirs:
            export_dir = export_dir.relative_to(self.flowDir.parent)
            self.tasksDict[task_name].exportSubDirs.append(export_dir)

    def __getitem__(self, taskName):
        return self.tasksDict[taskName]

    # def checkTaskFlowCount(self):
    #     print('Incorrect Counts')
    #     [t for t in self.tasks if not t.checkTaskFlowCount()]

    # def checkFlowFlowCount(self):
    #     [t.checkFlowFlowCount() for t in self.tasks]

    # def checkIndividualFiles(self):
    #     self.pathsDict = {}
    #     # ipt_files = flatten([t.iptFiles for t in self.headTasks])

    #     for ht in self.headTasks:
    #         for iptFile in ht.iptFiles:
    #             paths = ht.getPaths(iptFile.name)
    #             print(f'*** {iptFile.name} -> {len(paths)}')
    #             self.pathsDict[iptFile] = paths
    #     # end

    def show_mermaid(self):
        def get_task_line(t):
            _, ipt_count = t.get_input_ext_counts()
            _, opt_count = t.get_output_ext_counts()
            result = f'[<div align=leg>{t.name}</div><br/>'
            result += f'input: {ipt_count}<br/>output: {opt_count}]'
            return result

        repo_url = f'https://github.com/orgpedia/{self.org_code}/tree/main'
        s = '# Document Flow Diagram\n'
        s += 'This diagram is an auto-generated from directory structure of `flow` directory'
        s += ' and links present in `input` and `output` sub-folders (tasks). Click the box (task) to explore more.\n'
        s += '\n```mermaid\ngraph TD;\n'
        task_names = []


        ignore_tasks = ['RPS_List/buildOrder_', 'I.P.S/image/genOfficerID_', 'subFlows/hand']

        for (t1, t2) in [(t1, t2) for t1 in self.tasks for t2 in t1.downstreamTasks]:
            if 'ignore/' in t2.name:
                continue
            
            if t1.name in ignore_tasks or t2.name in ignore_tasks:
                continue

            t1_line = get_task_line(t1)
            t2_line = get_task_line(t2) if not t2.downstreamTasks else ""
            

            s += f'\t{t1.name}{t1_line} --> {t2.name}{t2_line};\n'
            task_names.append(t1.name)
            task_names.append(t2.name)

        imp_exp_links = []
        for task in (t for t in self.tasks if t.importSubDirs):
            for import_dir in task.importSubDirs:
                if task.name in ignore_tasks:
                    continue
                s += f'\t{import_dir} --> {task.name};\n'
                imp_exp_links.append(import_dir)

        for task in (t for t in self.tasks if t.exportSubDirs):
            for export_dir in task.exportSubDirs:
                s += f'\t{task.name} --> {export_dir};\n'
                imp_exp_links.append(export_dir)

                
        self.ignore_files, self.skipped_files, total_unprocessed = {}, {}, 0
        for task in self.tasks:
            if 'ignore/' in task.name:
                self.ignore_files[task.name] = len(task.iptFiles)
                total_unprocessed += len(task.iptFiles)
                continue

            if task.name in ignore_tasks:
                continue

            _, ipt_count = task.get_input_ext_counts()
            _, opt_count = task.get_output_ext_counts()
            if ipt_count != opt_count:
                self.skipped_files[task.name] =  ipt_count - opt_count
                total_unprocessed += ipt_count - opt_count

        task_names = set(task_names)
        imp_exp_links = set(imp_exp_links)
        s += '\n'.join(f'\tclick {n} "{repo_url}/flow/{n}" "{n}";' for n in task_names)
        s += '\n'
        s += '\n'.join(f'\tclick {d} "{repo_url}/{d}" "{d}";' for d in imp_exp_links)
        s += '\n```\n'

        s += f'## Unprocessed Documents: {total_unprocessed}\n'
        s += '### Ignored Documents:\n'
        s += '\n'.join(f'  - [{k}]({k}): {v}' for (k,v) in self.ignore_files.items())

        s += '\n### Skipped Documents:\n'        
        s += '\n'.join(f'  - [{k}]({k}): {v}' for (k,v) in self.skipped_files.items())        
        return s

    def show_readme(self):
        return self.show_mermaid()

    def show_summary(self):
        total_skipped, total_error = 0, 0
        s = '## Tasks\n'
        for task in self.tasks:
            task_skipped, task_error = len(task.get_skipped_docs()), task.get_total_errors()
            if task_skipped or task_error:
                s += f'### {task.name}\n'
                s += f'    skipped: {task_skipped}\n'
                s += f'    errors: {task_error} {task.get_error_summary()}\n\n'
            total_skipped += task_skipped
            total_error += task_error

        s += '\n## Summary\n'
        s += f'    skipped: {total_skipped}\n'
        s += f'    errors: {total_error}\n'
        return s

    def check_files(self):
        [t.check_files() for t in self.tasks]

def get_flow_task_dir():
    cwd = pathlib.Path.cwd()
    flow_dir, task_dir = '', ''

    if cwd.name == "flow":
        flow_dir = cwd
    elif (cwd / "flow").exists():
        flow_dir = cwd / "flow"
    else:
        flow_dir = [p for p in cwd.parents if p.name == "flow"]
        if not flow_dir:
            print(f"Unable to find 'flow' directory in the path: {cwd}")
            sys.exit(1)
        elif len(flow_dir) > 1:
            print(f"Multiple 'flow' directories found in the path: {cwd}")
            sys.exit(2)
        else:
            flow_dir = flow_dir[0]
            if cwd.name.endswith('_'):
                task_dir = cwd
            else:
                task_dir = first([p for p in cwd.parents if str(p).endswith('_')], '')

    return flow_dir, task_dir
    
    



if __name__ == "__main__":
    flow_dir, task_dir = get_flow_task_dir()

    if task_dir:
        task = Task(task_dir, flow_dir)

        if len(sys.argv) > 1 and sys.argv[1] == 'readme':
            readme_path = task_dir / 'README.md'
            readme_path.write_text(task.show_readme())
        else:
            # print(task.show_readme())
            task.check_files()

    else:
        flow = Flow(flow_dir)
        if len(sys.argv) > 1 and sys.argv[1] == 'readme':
            readme_path = flow_dir / 'README.md'
            readme_path.write_text(flow.show_readme())
        elif len(sys.argv) > 1 and sys.argv[1] == 'readme_all':
            readme_path = flow_dir / 'README.md'
            readme_path.write_text(flow.show_readme())
            for task in flow.tasks:
                task_readme_path = task.taskDir / 'README.md'
                task_readme_path.write_text(task.show_readme())
        else:
            # print(flow.show_readme())
            # print(flow.show_summary())
            # flow.checkFlowFlowCount()
            # flow.checkIndividualFiles()
            flow.check_files()
