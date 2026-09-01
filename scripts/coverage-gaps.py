"""Пер-строчные пробелы branch-покрытия, оба ассета, по базовым именам файлов."""
import glob, os, re
import xml.etree.ElementTree as ET

WANT = {
    'Mediator.cs', 'MedianaConfiguration.cs', 'ServiceCollectionExtensions.cs', 'MedianaDiagnostics.cs',
    'RequestCallSites.cs', 'EventCallSite.cs', 'StreamCallSite.cs', 'ChainState.cs', 'MessageRegistry.cs',
    'Envelope.cs', 'GuidV7.cs', 'Serialization.cs', 'RouteRegistry.cs', 'Transport.cs', 'InboxStore.cs',
    'Retry.cs', 'ConsumerPipeline.cs', 'OutboxRelay.cs',
}

def parse_cond(attr):
    m = re.search(r'\((\d+)/(\d+)\)', attr or '')
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

def report(cob_xml, label):
    root = ET.parse(cob_xml).getroot()
    per_file = {}
    for cls in root.iter('class'):
        base = os.path.basename(cls.attrib.get('filename', '').replace(chr(92), '/'))
        if base not in WANT:
            continue
        for line in cls.iter('line'):
            cc = line.attrib.get('condition-coverage')
            if cc is None:
                continue
            num, den = parse_cond(cc)
            if den > 0 and num < den:
                per_file.setdefault(base, {}).setdefault(line.attrib['number'], f'{num}/{den}')
    print(f'===== {label} =====')
    for base, lines in sorted(per_file.items()):
        items = ' '.join(f'L{n}({v})' for n, v in sorted(lines.items(), key=lambda x: int(x[0])))
        print(f'{base}: {items}')

for proj in ('Mediana.UnitTests', 'Mediana.UnitTests.Ns21'):
    fs = sorted(glob.glob(f'tests/{proj}/TestResults/*/coverage.cobertura.xml'))
    if fs:
        report(fs[-1], proj)
