import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent  # /app/config
PATH = BASE_DIR / "material_catalog.json"
def split_works_into_subgroups(node: dict):
    """
    Обрабатывает один узел, у которого может быть:
    - node["работы"]
    - node["подкатегории"]
    Превращает элементы с ценой 0 в подкатегории.
    """
    # Сначала рекурсивно пройдемся по уже существующим подкатегориям
    if "подкатегории" in node:
        for sub in node["подкатегории"]:
            split_works_into_subgroups(sub)

    if "работы" not in node:
        return

    works = node["работы"]
    new_subcats = []
    current_group = None
    direct_works = []  # работы, которые не попали ни под один заголовок-группу

    for item in works:
        price = item.get("цена")
        if isinstance(price, (int, float)) and price == 0:
            # Это заголовок подгруппы
            current_group = {
                "название": item["название"],
                "работы": []
            }
            new_subcats.append(current_group)
        else:
            # Обычная работа/товар
            if current_group is not None:
                current_group["работы"].append(item)
            else:
                # Если до этого не было заголовка с ценой 0 — оставляем как есть
                direct_works.append(item)

    # Если хотя бы одна подгруппа появилась — переносим структуру
    if new_subcats:
        # Добавляем новые подкатегории к существующим (если были)
        if "подкатегории" not in node:
            node["подкатегории"] = []
        node["подкатегории"].extend(new_subcats)

        # Если есть работы вне групп — оставим их в корне,
        # иначе поле "работы" можно убрать.
        if direct_works:
            node["работы"] = direct_works
        else:
            node.pop("работы", None)


def main():
    data = json.loads(PATH.read_text(encoding="utf-8"))

    # Верхний уровень — массив крупных групп
    for group in data:
        split_works_into_subgroups(group)

    PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


if __name__ == "__main__":
    main()