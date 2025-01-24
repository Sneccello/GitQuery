import os
import subprocess



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

    lock_path = os.path.join(repo_dir, '.git', 'index.lock')
    if os.path.exists(lock_path):
        subprocess.run(["rm", "-f", lock_path])

    git_log_command = [
        'git', 'log', '--name-status',
        '--pretty=format:commit: %H%nparents: %P%nmessage: %s%nauthor: %ae%ndate: %ad',
        '--date=iso'
    ]
    with open(output_path, 'w') as output_file:
        subprocess.run(git_log_command, cwd=repo_dir, stderr=output_file, stdout=output_file, check=True)





