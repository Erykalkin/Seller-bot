import os

def collect_py_files(root_dir=".", output_file="all_code.txt"):
    exclude_dirs = {"venv", "__pycache__", ".git", ".idea", ".vscode", ".txt", ".json", ".ipynb", ".log", ".gitattributes", ".gitignore"}

    with open(output_file, "w", encoding="utf-8") as out:
        for folder, subfolders, files in os.walk(root_dir):
            # исключаем нежелательные каталоги из обхода
            subfolders[:] = [d for d in subfolders if d not in exclude_dirs]

            # только .py
            py_files = [f for f in files if f.lower().endswith(".py")]
            py_files.sort()

            for file in py_files:
                file_path = os.path.join(folder, file)
                rel_path = os.path.relpath(file_path, root_dir)
                out.write(f"[{rel_path}]\n")  # относительный путь удобнее, чем только имя
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        out.write(f.read())
                except UnicodeDecodeError:
                    with open(file_path, "r", encoding="latin-1") as f:
                        out.write(f.read())
                out.write("\n\n")  # разделитель между файлами


def build_project_tree(root_dir=".", output_file="project_tree.txt"):
    exclude_dirs = {"venv", "__pycache__", ".git", ".idea", ".vscode"".ipynb", ".log", ".gitattributes", ".gitignore", "old"}

    def _tree(dir_path, prefix=""):
        entries = []
        with os.scandir(dir_path) as it:
            for entry in it:
                if entry.is_dir():
                    if entry.name in exclude_dirs:
                        continue
                    entries.append((entry.name, True))
                else:
                    # теперь добавляем **все файлы**
                    entries.append((entry.name, False))

        # сортировка: папки сверху, файлы снизу
        entries.sort(key=lambda x: (not x[1], x[0].lower()))

        lines = []
        for i, (name, is_dir) in enumerate(entries):
            connector = "└── " if i == len(entries) - 1 else "├── "
            line = f"{prefix}{connector}{name}"
            lines.append(line)
            if is_dir:
                extension = "    " if i == len(entries) - 1 else "│   "
                lines.extend(_tree(os.path.join(dir_path, name), prefix + extension))
        return lines

    lines = [os.path.basename(os.path.abspath(root_dir)) or root_dir]
    lines.extend(_tree(root_dir))
    tree_text = "\n".join(lines)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(tree_text)
    return tree_text


# === запуск ===
project_dir = "."
code_out = "all_code.txt"
tree_out = "project_tree.txt"

collect_py_files(project_dir, code_out)
tree_str = build_project_tree(project_dir, tree_out)

print(f"✅ Код из всех .py файлов сохранён в {code_out}")
print(f"✅ Дерево проекта сохранено в {tree_out}")
print()
print(tree_str)