import math


def clamp_page(page: int | None, total_pages: int) -> int:
    """Нормализует номер страницы в пределах [0, total_pages - 1]."""
    if total_pages <= 0:
        return 0
    page = int(page or 0)
    return max(0, min(page, total_pages - 1))


def total_pages_for(total_items: int, page_size: int) -> int:
    """Возвращает количество страниц для заданного размера страницы."""
    if page_size <= 0:
        return 1
    return max(1, math.ceil(total_items / page_size))


def paginate_list(items: list, page: int, page_size: int) -> tuple[list, int, int]:
    """Возвращает элементы текущей страницы и метаданные (page, total_pages)."""
    total_pages = total_pages_for(len(items), page_size)
    page = clamp_page(page, total_pages)
    start = page * page_size
    end = start + page_size
    return items[start:end], page, total_pages
