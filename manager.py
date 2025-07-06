import shutil

from pathlib import Path
from typing import Dict, Union


class FileManager:
    @staticmethod
    def write_file(data: Dict[str, str], file_path: Union[Path, str]) -> None:
        text = "\n".join(f"{key}: {value}" for key, value in data.items())

        if not isinstance(file_path, Path):
            file_path = Path(file_path)

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w") as file:
            file.write(text)


class DirManager:
    @staticmethod
    def move_to_numbered_dir(source_dir: Path, target_dir: Path) -> Path:
        """
        Moves files from source_dir to a new numbered subdirectory in target_dir.
        Returns path to the created subdirectory.

        Example:
            If target_dir contains folders [1, 2], creates folder 3 and moves files there.
            If target_dir is empty, creates folder 1.
        """
        if not source_dir.exists():
            raise FileNotFoundError(f"Source directory not found: {source_dir}")

        if not source_dir.is_dir():
            raise NotADirectoryError(f"Source path is not a directory: {source_dir}")

        # Ensure target_dir exists
        target_dir.mkdir(parents=True, exist_ok=True)

        # Find existing numbered folders
        existing_folders = []
        for item in target_dir.iterdir():
            if item.is_dir() and item.name.isdigit():
                existing_folders.append(int(item.name))

        # Determine next folder number
        next_number = max(existing_folders) + 1 if existing_folders else 1
        new_folder = target_dir / str(next_number)
        new_folder.mkdir()

        # Move files
        for item in source_dir.iterdir():
            if item.is_file():
                shutil.move(str(item), str(new_folder / item.name))

        return new_folder

    @staticmethod
    def clear_directory(folder_path: Path) -> None:
        """
        Полностью очищает указанную папку, удаляя все её содержимое
        (файлы и подпапки), но сохраняет саму папку.

        Args:
            folder_path: Путь к папке, которую нужно очистить

        Raises:
            FileNotFoundError: Если папка не существует
            NotADirectoryError: Если путь ведёт не к папке
        """
        if not folder_path.exists():
            raise FileNotFoundError(f"Папка не найдена: {folder_path}")
        if not folder_path.is_dir():
            raise NotADirectoryError(f"Указанный путь не является папкой: {folder_path}")

        # Удаляем всё содержимое папки
        for item in folder_path.iterdir():
            if item.is_file():
                item.unlink()  # Удаляем файл
            elif item.is_dir():
                shutil.rmtree(item)
