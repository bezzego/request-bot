"""Скрипт для обновления material_catalog.json: замена 'работы' на 'материалы'."""
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PATH = BASE_DIR / "material_catalog.json"


def replace_works_to_materials(node: dict) -> None:
    """Рекурсивно заменяет 'работы' на 'материалы' в узле."""
    # Обрабатываем подкатегории
    if "подкатегории" in node:
        for sub in node["подкатегории"]:
            replace_works_to_materials(sub)
    
    # Заменяем 'работы' на 'материалы'
    if "работы" in node:
        node["материалы"] = node.pop("работы")


def main():
    """Обновляет material_catalog.json."""
    data = json.loads(PATH.read_text(encoding="utf-8"))
    
    for group in data:
        replace_works_to_materials(group)
    
    PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print("✅ material_catalog.json обновлён: 'работы' заменены на 'материалы'")


if __name__ == "__main__":
    main()

