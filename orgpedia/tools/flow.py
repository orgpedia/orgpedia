import os
import pathlib
import sys
from collections import Counter
from more_itertools import pairwise, first

import yaml
import graphviz


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

def get_link_src_subdir(link_files, parent_dir):
    num_dirs = len(parent_dir.parents)
    srcs = [get_link_src(f) for f in link_files if f.is_symlink()]
    mat_srcs = [ s for s in srcs if parent_dir in s.parents ]
    mat_subdirs = set(s.parents[len(s.parents) - (num_dirs + 2)] for s in mat_srcs)
    return mat_subdirs

class ErrorInfo:
    def __init__(self, doc_name, error_line):
        self.doc_name = doc_name
        error_line = error_line[2:].replace('=', ' ').replace('Total:', 'Total ')
        error_fields = error_line.split(' ')

        self.total_entities = int(error_fields[1])
        self.total_errors = int(error_fields[3])
        self.error_counts = {}
        for error_type, error_count in pairwise(error_fields[4:]):
            self.error_counts[error_type] = error_count

    @property
    def error_pair(self):
        return (self.total_entities, self.total_errors)

        
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
        #end
        self.errors = None


    @property
    def full_name(self):
        return f"{self.task.name}->{self.name}"

    def parse_error_line(self, error_line):
        error_line = error_line[2:].replace('=' ' ')
        
    def get_errors(self):
        def is_error(line):
            return line.startswith('==') and 'Errors' in line

        def get_doc_name(log_file):
            pdf_idx = log_file.name.lower().find('.pdf')
            return log_file.name[:pdf_idx + 4]

        if not self.stub:
            return []
        
        log_dir = self.task.taskDir / 'logs'
        log_files = log_dir.glob(f'*.{self.stub}.*')

        errors = []
        for log_file in log_files:
            doc_name = get_doc_name(log_file)
            error_line = [l for l in log_file.read_text().split('\n') if is_error(l)]
            if error_line:
                error_line = error_line[0]
                error_info = ErrorInfo(doc_name, error_line)
                errors.append(error_info)
        return errors

    def get_error_percent(self):
        if self.errors is None:
            self.errors = self.get_errors()

        if self.errors:
            error_pairs = [e.error_pair for e in self.errors]
            total_entities = [e[0] for e in error_pairs ]
            total_errors = [ e[1] for e in error_pairs ]
            if sum(total_entities) != 0:
                return (sum(total_errors)/sum(total_entities)) * 100
            else:
                return 0.0
        else:
            return 0.0

    def get_total_entities(self):
        if self.errors is None:
            self.errors = self.get_errors()

        if self.errors:
            error_pairs = [e.error_pair for e in self.errors]
            total_entities = [e[0] for e in error_pairs ]
            return sum(total_entities)
        else:
            return 0.0

    def get_total_errors(self):
        if self.errors is None:
            self.errors = self.get_errors()

        if self.errors:
            error_pairs = [e.error_pair for e in self.errors]
            total_errors = [e[1] for e in error_pairs ]
            return sum(total_errors)
        else:
            return 0.0
        
        

class Task:
    def __init__(self, taskDir, pipelineDir):
        # if taskDir.name == 'buildTenure_':
        #     print("Found It")
        
        self.taskDir = taskDir
        self.pipelineDir = pipelineDir
        self.name = self._getTaskName(self.taskDir)

        self.iptFiles = self._getTaskFiles('input')
        self.optFiles = self._getTaskFiles('output')

        self.upstream = self._getUpTasks()
        self.downstreamTasks = []

        self.skipped_docs = {}                
        self.sub_tasks = self._getSubTasks()

        importDir = pipelineDir.parent / 'import'

        i_paths = get_link_src_subdir(self.iptFiles, importDir)
        self.importSubDirs = [i.relative_to(self.pipelineDir.parent) for i in i_paths]
        self.exportSubDirs = []

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
        opt_ext, _  = self.get_output_ext_counts()
        return list((e, c) for (e, c) in ext_counts.items() if e != opt_ext)

    def get_skipped_docs(self):
        result = []
        for ipt_file, skipped_info in self.skipped_docs.items():
            if isinstance(skipped_info, list):
                reason = f'Skipped in {ipt_file}'
                result += [ (s, reason) for s in skipped_info ]
            elif isinstance(skipped_info, dict):
                result += [ (s, f'{ipt_file} {r}') for s, r in skipped_info.items() ]
            else:
                raise NotImplementedError(f'Unknown type {type(skipped_info)}')
        return result

    @property
    def sub_task_full_names(self):
        return [st.full_name for st in self.sub_tasks]


    def addDwnTask(self, task):
        if id(task) not in [id(t) for t in self.downstreamTasks]:
            self.downstreamTasks.append(task)

    def get_total_errors(self):
        return sum(st.get_total_errors() for st in self.sub_tasks)

    def get_error_summary(self):
        return ', '.join(f'{st.name}: {st.get_total_errors()}' for st in self.sub_tasks if st.get_total_errors())

    def _getTaskName(self, dirPath):
        dirPathStr = str(dirPath)
        pipelinePathStr = str(self.pipelineDir)
        if pipelinePathStr in dirPathStr:
            taskName = dirPathStr[len(pipelinePathStr)+1:]
            taskName = taskName[:taskName.rindex("_")+1]
            return taskName            
        else:
            return ''

    def _getTaskFiles(self, subdir_name):
        if subdir_name == 'input':
            subDir = self.taskDir / subdir_name
            linkFiles = subDir.glob("*")
            linkFiles = [l for l in linkFiles if l.is_symlink() and not l.name.startswith('.') ]
            return linkFiles
        elif subdir_name == 'output':
            subDir = self.taskDir / subdir_name
            optFiles = list(subDir.glob("*.json")) + list(subDir.glob("*.html"))
            return optFiles
        else:
            raise NotImplementedError('Cannot list files in {subdir_name}')
            

    def _getUpTasks(self):
            
        srcIptFiles = [ get_link_src(l) for l in self.iptFiles ]
        srcParentDirs = set([s.parent for s in srcIptFiles])
        taskNames = set([self._getTaskName(pDir) for pDir in srcParentDirs])
        taskNames = [ tName for tName in taskNames if tName ]
        return taskNames
    
    def _getSubTasks(self):
        def read_sub_tasks(sub_task_file):
            sub_task_files = [ sub_task_file ]
            if sub_task_file.name == '.info.yml':
                sub_task_names = sub_task_file.read_text().split('\n')
                sub_task_files = [ sub_task_file.parent / n for n in sub_task_names if n]

            all_sub_tasks = []
            for st_file in sub_task_files:
                yml_dict = yaml.load(st_file.read_text(), Loader=yaml.FullLoader)
                sub_dicts = yml_dict['pipeline']
                sub_tasks = [ SubTask(yt, self, st_file) for yt in sub_dicts ]
                sub_tasks = [ st for st in sub_tasks if st.name != 'html_generator']
                self.skipped_docs[st_file.name] = yml_dict.get('ignore_docs', []) # TODO
                [st.get_errors() for st in sub_tasks]

                
                all_sub_tasks.extend(sub_tasks)
            return all_sub_tasks
        
        src_dir = self.taskDir  / 'src'
        yml_files = list(src_dir.glob('*.yml'))
        info_file_path = src_dir / '.info.yml'

        if len(yml_files) == 0:
            return []
        elif len(yml_files) == 1:
            return read_sub_tasks(yml_files[0])
        elif info_file_path in yml_files:
            return read_sub_tasks(info_file_path)
        else:
            raise ValueError(f'Unable to find info.yml in {str(src_dir)}')

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


    def show_mermaid(self):
        s = '```mermaid\ngraph TD;\n'
        for (st1, st2) in pairwise(self.sub_tasks):            
            total_entities = st1.get_total_entities()
            error_percent = st1.get_error_percent()
            total_str = total_entities if total_entities else ''
            error_str = f'{error_percent:.2f}%' if error_percent else ''
            
            t1_l1 = f'<div align=left>{st1.name}<div/><br/>'
            t1_l2 = f'Total: {total_str}[{error_str}]'
            s += f'\t{st1.name} [{t1_l1}{t1_l2}] --> {st2.name};\n'
        s += '```\n'
        print(s)

    def show_counts(self):
        ipt_ext, ipt_count = self.get_input_ext_counts()
        opt_ext, opt_count = self.get_output_ext_counts()
        intermediates = self.get_intermediates_ext_counts()
        
        s  =  '| Directory    | Files                          | Counts |\n'
        s +=  '|--------------|--------------------------------|--------|\n'
        s += f'| input        | {ipt_ext:30} | {ipt_count:>6} |\n'
        s += f'| output       | {opt_ext:30} | {opt_count:>6} |\n'

        for i_ext, i_count in intermediates:
            s += f'| intermediate | {i_ext:30} | {i_count:>6} |\n'
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
            ent_tot, err_per = sub_task.get_total_entities(), sub_task.get_error_percent()
            err_tot = sub_task.get_total_errors()
            s += f'\n### {sub_task.name}\n'
            s += f'    Entities: {ent_tot:,} Errors: {err_tot:,} [{err_per:.2f}%]\n'

        return s

    def show_readme(self):
        s = f'# {task.name}\n\n'
        s += self.show_counts() + '\n'
        s += self.show_skipped_docs() + '\n'
        s += self.show_sub_tasks() + '\n'
        return s

    def checkPipelineFlowCount(self):
        def is_multiple(x, m):
            return x and (m % x) == 0
        
        tskOutput = len(self.optFiles)
        dwnInput = sum([len(dTask.iptFiles) for dTask in self.downstreamTasks])

        if not is_multiple(tskOutput, dwnInput):
            print(f'Mismatch: {self.name} output: {tskOutput} dwnInput: {dwnInput}')


    def checkTaskFlowCount(self):
        if len(self.iptFiles) == len(self.optFiles):
            return True
        elif self.iptFiles and not self.optFiles:
            print(f'Task Flow Mismatch: {self.name} ipt:{len(self.iptFiles)} opt:{len(self.optFiles)}')
            return False
        else:
            ipt_exts = [ get_all_exts(f) for f in self.iptFiles if get_all_exts(f) ]
            opt_exts = [ get_all_exts(f) for f in self.optFiles if get_all_exts(f) ]

            ipt_exts_ctr = Counter([ ext for ext in ipt_exts])
            opt_exts_ctr = Counter([ ext for ext in opt_exts])
            
            ipt_flow = len(ipt_exts)/len(ipt_exts_ctr)
            opt_flow = len(opt_exts)/len(opt_exts_ctr)
            
            if ipt_flow != opt_flow:
                print(f'Task Flow Mismatch: {self.name}:\n\tipt[{len(self.iptFiles)}]:{ipt_exts_ctr}\n\topt:{opt_exts_ctr}')
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
            return any([pdfFile in str(fName) for fName in fileNames ])

        #if pdfFile == '1_Upload_1546.pdf' and self.name == 'genHtml_':
        #    print('Found it')
        
        paths = []
        print(f'Exploring {self.name} [{pdfFile}]')
        if hasStub(self.iptFiles, pdfFile)  and hasStub(self.optFiles, pdfFile):
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
    
class Pipeline:
    def __init__(self, pipelineDir):
        self.pipelineDir = pipelineDir
        self.org_code = pipelineDir.parent.name
        self.tasks = self._buildTasks()
        self.tasksDict = dict([(t.name, t) for t in self.tasks])
        self.populateDownstream()
        
        self.headTasks = [t for t in self.tasks if not t.upstream]
        self.tailTasks = [t for t in self.tasks if not t.downstreamTasks]
        self.populateExportDir()

    def _buildTasks(self):
        def isTask(taskDir):
            if '.bak' in taskDir.parts:
                return False
            subDirs = taskDir.glob("*")
            matchedDirs = [p for p in subDirs if p.name in ("input", "output")]
            return True if len(matchedDirs) == 2 else False

        tasks = []
        for p in self.pipelineDir.glob("*/**"):
            if p.is_dir() and p.name.endswith("_") and isTask(p):

                tasks.append(Task(p, self.pipelineDir))
        return tasks

    def populateDownstream(self):
        for task in self.tasks:
            [ self.tasksDict[uptaskName].addDwnTask(task) for uptaskName in task.upstream ]

    def populateExportDir(self):
        def getTaskName(dir_path):
            rel_path = dir_path.parent.relative_to(self.pipelineDir)
            return str(rel_path)
        
        export_dir = self.pipelineDir.parent / 'export'
        exp_files = (f for f in export_dir.glob('**/*') if not f.is_dir())
        
        src_dirs_exp_dirs = set((get_link_src(f).parent, f.parent) for f in exp_files)
        task_names_exp_dirs = set((getTaskName(s), e) for s, e in src_dirs_exp_dirs)
        for task_name, export_dir in task_names_exp_dirs:
            export_dir = export_dir.relative_to(self.pipelineDir.parent)
            self.tasksDict[task_name].exportSubDirs.append(export_dir)

    def __getitem__(self, taskName):
        return self.tasksDict[taskName]

    def checkTaskFlowCount(self):
        print('Incorrect Counts')
        [ t for t in self.tasks if not t.checkTaskFlowCount() ]

    def checkPipelineFlowCount(self):
        [ t.checkPipelineFlowCount() for t in self.tasks ]

    def checkIndividualFiles(self):
        self.pathsDict = {}
        
        for iptFile in self.headTask.iptFiles:
            paths = self.headTask.getPaths(iptFile.name)
            print(f'*** {iptFile.name} -> {len(paths)}')
            self.pathsDict[iptFile] = paths
        #end

    def show_mermaid(self):
        def get_task_line(t):
            _, ipt_count = t.get_input_ext_counts()
            _, opt_count = t.get_output_ext_counts()
            result =  f'[<div align=leg>{t.name}</div><br/>'
            result += f'Input: {ipt_count}<br/>Output: {opt_count}]'
            return result

        repo_url = f'https://github.com/orgpedia/{self.org_code}/tree/main'
        s = '```mermaid\ngraph TD;\n'        
        task_names = []

        ignore_tasks = ['RPS_List/buildOrder_', 'I.P.S/image/genOfficerID_']
        
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
                

        task_names = set(task_names)
        imp_exp_links = set(imp_exp_links)
        s += '\n'.join(f'\tclick {n} "{repo_url}/flow/{n}" "{n}";' for n in task_names)
        s += '\n'
        s += '\n'.join(f'\tclick {d} "{repo_url}/{d}" "{d}";' for d in imp_exp_links)
        s += '\n```\n'
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
        

    def show(self, renderFile):
        orgCode = self.pipelineDir.parent.name
        dot = graphviz.Digraph(name=orgCode)
        dot.attr('node', width='3.0', fixedsize='true', shape='plaintext')

        for task in self.tasks:
            t_name = f'cluster_{task.name}'
            with dot.subgraph(name=t_name) as task_graph:
                task_graph.attr(label=f"{task.name}")
                [ task_graph.node(st.full_name, st.name) for st in task.sub_tasks]
                task_graph.edges(pairwise(task.sub_task_full_names))

        for (t1, t2) in [(t1, t2) for t1 in self.tasks for t2 in t1.downstreamTasks]:
            n1, n2 = t1.sub_tasks[-1].full_name, t2.sub_tasks[0].full_name
            dot.edge(n1, n2)
            
        print(dot.source)

        renderFile = pathlib.Path(renderFile)
        dot.render(renderFile.stem, format="png")
        
        

    def render(self, renderFile):
        orgCode = self.pipelineDir.parent.name
        
        def label(name):
            return pathlib.Path(name).parts[-1]

        def subgraph(name):
            parts = pathlib.Path(name).parts
            print(f'** {name} -> {parts}')
            if len(parts) > 1:
                return str(pathlib.Path(*parts[:-1]))
            else:
                return orgCode

        def parentSubgraph(s):
            if s == 'orgCode':
                return None
            else:
                return subgraph(s)
            
        # create base graph
        dot = graphviz.Digraph(name=orgCode)

        
        edges = [(uName, t.name) for t in self.tasks for uName in t.upstream]

        # identify subgraphNames the tasks that are in the directory
        subgraphs = set([subgraph(t.name) for t in self.tasks])
        graphDict = dict([ (s, graphviz.Digraph(name=f'cluster_{s}',graph_attr={'label':s})) for s in subgraphs if s != orgCode] )

        # add all nodes to the base graph
        #[dot.node(t.name) for t in self.tasks]
        for t in self.tasks:
            subgraphName = subgraph(t.name)
            if subgraphName == orgCode:
                dot.node(t.name)
            else:
                graphDict[subgraphName].node(t.name)
        

        #add the base graph as well
        graphDict[orgCode] = dot
        
        for (n1, n2) in edges:
            s1, s2 = subgraph(n1), subgraph(n2)
            if s1 == s2:
                graphDict[s1].edge(n1, n2)                
            else:
                graphDict[orgCode].edge(n1, n2)
        #end for

        for g, gDot in graphDict.items():
            parent = parentSubgraph(g)
            if parent and parent != g:
                graphDict[parent].subgraph(gDot)
        print(dot.source)

        renderFile = pathlib.Path(renderFile)
        dot.render(renderFile.stem, format="png")


if __name__ == "__main__":
    cwd = pathlib.Path.cwd()

    pipeline_dir, task_dir = '', ''

    if cwd.name == "pipeline" or cwd.name == "flow":
        pipeline_dir = cwd
    else:
        pipeline_dir = [p for p in cwd.parents if p.name in ("pipeline", "flow")]
        if not pipeline_dir:
            print(f"Unable to find 'pipeline' directory in the path: {cwd}")
            sys.exit(1)
        elif len(pipeline_dir) > 1:
            print(f"Multiple 'pipeline' directories found in the path: {cwd}")
            sys.exit(2)
        else:
            pipeline_dir = pipeline_dir[0]
            if cwd.name.endswith('_'):
                task_dir = cwd
            else:
                task_dir = first([ p for p in cwd.parents if str(p).endswith('_')], '')

    if task_dir:
        pipeline = Pipeline(pipeline_dir)
        task = first((t for t in pipeline.tasks if t.taskDir == task_dir), None)
        
        if len(sys.argv) > 1 and sys.argv[1] == 'readme':
            readme_path = task_dir / 'README.md'
            readme_path.write_text(task.show_readme())
        else:
            print(task.show_readme())

    else:
        pipeline = Pipeline(pipeline_dir)
        if len(sys.argv) > 1 and sys.argv[1] == 'readme':        
            readme_path = pipeline_dir / 'README.md'
            readme_path.write_text(pipeline.show_readme())
        elif len(sys.argv) > 1 and sys.argv[1] == 'readme_all':        
            readme_path = pipeline_dir / 'README.md'
            readme_path.write_text(pipeline.show_readme())
            for task in pipeline.tasks:
                task_readme_path = task.taskDir / 'README.md'
                task_readme_path.write_text(task.show_readme())
        else:
            #print(pipeline.show_readme())
            print(pipeline.show_summary())

            
    
            

        

