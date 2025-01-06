import subprocess

import git
from git import RemoteProgress

class CloneProgress(RemoteProgress):
    def __init__(self):
        super().__init__()
        self.progress = 0

    def update(self, op_code, cur_count, max_count=None, message=''):
        if max_count:
            self.progress = (cur_count / max_count) * 100


def get_repo_id(repo_link):
    url_parts = repo_link.rstrip('/').strip('.git').split("/")
    repo_name = url_parts[-1]
    owner = url_parts[-2]
    return f'{owner}_{repo_name}'

def get_git_repo_link(repo_or_gitfile: str):
    if repo_or_gitfile.endswith(".git"):
        return repo_or_gitfile.rstrip(".git")
    return repo_or_gitfile


def create_gitlog_file(repo_dir: str, output_path: str):
    repo = git.Repo(repo_dir)
    try:
        repo.git.checkout('main')
    except:
        repo.git.checkout('master')

    git_log_command = [
        'git', 'log', '--name-status',
        '--pretty=format:commit: %H%nparents: %P%nmessage: %s%nauthor: %ae%ndate: %ad',
        '--date=iso'
    ]
    with open(output_path, 'w') as output_file:
        subprocess.run(git_log_command, cwd=repo_dir, stderr=output_file, stdout=output_file, check=True)





