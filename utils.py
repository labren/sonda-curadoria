import os
from rich import print
from rich.tree import Tree
from rich.table import Table
import glob

def get_dir_info(path):
    total_size = sum(os.path.getsize(os.path.join(r, f))
                     for r, _, files in os.walk(path) for f in files)
    file_count = sum(len(files) for _, _, files in os.walk(path))
    return total_size / (1024 * 1024), file_count  # Retorna tamanho em MB e contagem de arquivos

def build_tree(path, tree, show_size=True, min_size_mb=0, filter_pattern=None, show_files=False):
    if filter_pattern:
        # Usa glob para filtrar os caminhos que correspondem ao padrão
        filtered_paths = glob.glob(os.path.join(path, filter_pattern), recursive=True)
        filtered_items = [os.path.relpath(p, path) for p in filtered_paths]
    else:
        # Se não houver filtro, lista todos os itens
        filtered_items = sorted(os.listdir(path))

    for item in filtered_items:
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path):  # Subdiretório
            size, count = get_dir_info(item_path)
            if size >= min_size_mb:  # Verifica se o tamanho é maior ou igual ao mínimo
                info = f"[green]{size:.2f} MB[/green]" if show_size else f"[magenta]{count} files[/magenta]"
                branch = tree.add(f":open_file_folder: [bold blue]{item}/[/] ({info})")
                build_tree(item_path, branch, show_size, min_size_mb, filter_pattern, show_files)
        elif show_files and os.path.isfile(item_path):  # Arquivo
            if show_size:
                size = os.path.getsize(item_path) / (1024 * 1024)
                tree.add(f":page_facing_up: {item} ([green]{size:.2f} MB[/green])")
            else:
                tree.add(f":page_facing_up: {item}")

def build_table(path, table, show_size=True, parent_dir="", min_size_mb=0, filter_pattern=None, show_files=False):
    if filter_pattern:
        # Usa glob para filtrar os caminhos que correspondem ao padrão
        filtered_paths = glob.glob(os.path.join(path, filter_pattern), recursive=True)
        filtered_items = [os.path.relpath(p, path) for p in filtered_paths]
    else:
        # Se não houver filtro, lista todos os itens
        filtered_items = sorted(os.listdir(path))

    for item in filtered_items:
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path):  # Subdiretório
            size, count = get_dir_info(item_path)
            if size >= min_size_mb:  # Verifica se o tamanho é maior ou igual ao mínimo
                info = f"{size:.2f} MB" if show_size else f"{count} files"
                table.add_row(f"{parent_dir}/{item}", info)
                build_table(item_path, table, show_size, f"{parent_dir}/{item}", min_size_mb, filter_pattern, show_files)
        elif show_files and os.path.isfile(item_path):  # Arquivo
            if show_size:
                size = os.path.getsize(item_path) / (1024 * 1024)
                table.add_row(f"{parent_dir}/{item}", f"{size:.2f} MB")
            else:
                table.add_row(f"{parent_dir}/{item}", "N/A")

def display_directories(root_dir, show_size=True, use_table=False, min_size_mb=0, filter_pattern=None, show_files=False):
    if use_table:
        # Criar a tabela
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Path", style="dim")
        table.add_column("Size / File Count")
        build_table(root_dir, table, show_size, min_size_mb=min_size_mb, filter_pattern=filter_pattern, show_files=show_files)
        print(table)
    else:
        # Criar a árvore
        root_name = os.path.basename(os.path.normpath(root_dir))
        tree = Tree(f":open_file_folder: [bold red]{root_name}/[/]")
        build_tree(root_dir, tree, show_size, min_size_mb=min_size_mb, filter_pattern=filter_pattern, show_files=show_files)
        print(tree)

# import os
# from utils import display_directories

# # Diretório base (ex: ~/sonda/)
# root_dir = os.path.expanduser("~/sonda/")

# # Parâmetros
# show_size = True  # Se True, mostra o tamanho; se False, mostra o número de arquivos
# use_table = False  # Se True, usa a tabela, caso contrário, usa a árvore
# min_size_mb = 1  # Define o tamanho mínimo do diretório em MB (diretórios menores que isso não serão exibidos)
# filter_pattern = None  # Filtra subdiretórios/arquivos que correspondem ao padrão
# show_files = False  # Se True, exibe os arquivos; se False, oculta os arquivos

# # Chama a função com a opção desejada
# display_directories(root_dir, show_size=show_size, use_table=use_table, min_size_mb=min_size_mb, filter_pattern=filter_pattern, show_files=show_files)