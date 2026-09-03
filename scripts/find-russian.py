"""Находит все блоки русского текста в .cs файлах и выводит файл + строку + текст."""
import re, os, sys, glob

RUSSIAN = re.compile(r'[а-яА-ЯёЁ]')
BASE = r'F:\Projects\Mediana'

def scan_files():
    results = []
    for root in ('src', 'tests', 'benchmarks'):
        for dirpath, _, files in os.walk(os.path.join(BASE, root)):
            if 'obj' in dirpath or 'bin' in dirpath:
                continue
            for f in files:
                if not f.endswith('.cs'):
                    continue
                path = os.path.join(dirpath, f)
                try:
                    lines = open(path, encoding='utf-8').readlines()
                except Exception:
                    continue
                for i, line in enumerate(lines, 1):
                    if RUSSIAN.search(line):
                        results.append((path, i, line.rstrip()))
    return results

if __name__ == '__main__':
    results = scan_files()
    current_file = None
    count = 0
    for path, line_no, text in results:
        rel = os.path.relpath(path, BASE)
        if rel != current_file:
            current_file = rel
            print(f"\n=== {rel} ===")
        print(f"  L{line_no}: {text.strip()[:120]}")
        count += 1
    print(f"\nTotal: {count} lines with Russian text in {len(set(r[0] for r in results))} files")
