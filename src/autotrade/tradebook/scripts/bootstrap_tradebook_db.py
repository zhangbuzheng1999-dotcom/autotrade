from __future__ import annotations

from autotrade.tradebook.storage.mongo import bootstrap_tradebook_collections


def main() -> None:
    collections = bootstrap_tradebook_collections()
    print("tradebook collections ready:")
    for name in collections:
        print(name)


if __name__ == "__main__":
    main()
