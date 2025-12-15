"""Устарело: материалынй каталог перенесён в mat.json."""
import sys


def main() -> None:
    msg = "material_catalog.json больше не используется; данные берутся из mat.json."
    print(msg)
    sys.exit(0)


if __name__ == "__main__":
    main()
