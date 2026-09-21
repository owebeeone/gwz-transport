#!/usr/bin/env python3
"""Regenerate transport artifacts using taut-proto==0.9.1; ordinary builds need no Python."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def verify_formatter(pin):
    actual = subprocess.check_output(['rustfmt', '--version'], text=True).strip()
    if actual != pin['rustfmt']:
        raise SystemExit(f"Expected rustfmt {pin['rustfmt']!r}; got {actual!r}")


def verify_taut_source(source, pin):
    source = source.resolve()
    if source.name != 'src' or not (source / 'taut').is_dir():
        raise SystemExit(f'Expected canonical taut source directory; got {source}')
    repo = Path(subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', '--show-toplevel'], text=True,
    ).strip()).resolve()
    expected_revision = pin.get('taut-source-revision')
    if expected_revision:
        revision = subprocess.check_output(
            ['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True,
        ).strip()
        if revision != expected_revision:
            raise SystemExit(
                f'Expected taut source revision {expected_revision!r}; got {revision!r}'
            )
    dirty = subprocess.check_output(
        ['git', '-C', str(repo), 'status', '--porcelain', '--untracked-files=all', '--', 'src'],
        text=True,
    ).strip()
    if dirty:
        raise SystemExit(f'Pinned taut source checkout is dirty under src/: {dirty}')
    for relative, expected in pin.get('taut-extension-sha256', {}).items():
        path = repo / relative
        if not path.is_file():
            raise SystemExit(f'Missing pinned taut extension: {path}')
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise SystemExit(
                f'Pinned taut extension digest mismatch for {relative}: '
                f'expected {expected}, got {actual}'
            )
    return source


def verify_taut_module(name, module, source):
    origin = getattr(module, '__file__', None)
    if not isinstance(origin, str):
        raise SystemExit(f'Imported {name} has no file origin')
    path = Path(origin).resolve()
    if path != source and source not in path.parents:
        raise SystemExit(f'Imported {name} is outside pinned taut source: {path}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--taut-source', type=Path,
                        help='canonical taut checkout src directory')
    args = parser.parse_args()
    pin = json.loads((ROOT / 'protocol/generator.json').read_text())
    if args.taut_source is None and pin.get('taut-source-revision'):
        raise SystemExit(
            'This schema requires the pinned local taut source; '
            'pass --taut-source <checkout>/src'
        )
    if args.taut_source is not None:
        source = verify_taut_source(args.taut_source, pin)
        loaded = [name for name in sys.modules
                  if name == 'taut' or name.startswith('taut.')]
        if loaded:
            raise SystemExit('taut modules are already loaded; use a fresh interpreter')
        sys.path.insert(0, str(source))
    import taut
    from admission_codegen import emit as admission
    from taut.gen.scaffold import emit
    from taut.ir.export import schema_json
    from taut.ir.load import load_schema
    from taut.ir.validate import validate_or_raise
    if args.taut_source is not None:
        for name, module in [('taut', taut),
                             ('taut.gen.scaffold', sys.modules['taut.gen.scaffold'])]:
            verify_taut_module(name, module, source)
    if args.taut_source is None and taut.__version__ != pin['taut-proto']:
        raise SystemExit(f"Expected taut-proto=={pin['taut-proto']}; got {taut.__version__}")
    verify_formatter(pin)
    schema = load_schema(ROOT / 'protocol/transport.taut.py')
    validate_or_raise(schema)
    with tempfile.TemporaryDirectory() as tmp:
        generated = Path(tmp)
        emit(schema, generated, langs=['rust'], services=[], runtime=True)
        (generated / 'admission.rs').write_text(admission(schema))
        # Formatting is part of reproducible generation, never a hand edit.
        subprocess.run(['rustfmt', '--edition=2024', str(generated / 'rust/api.rs'),
                        str(generated / 'rust/cbor.rs'), str(generated / 'admission.rs')], check=True)
        outputs = {
            ROOT / 'src/admission.rs': (generated / 'admission.rs').read_text(),
            ROOT / 'src/protocol.rs': (generated / 'rust/api.rs').read_text(),
            ROOT / 'src/cbor.rs': (generated / 'rust/cbor.rs').read_text(),
            ROOT / 'protocol/transport.ir.json': json.dumps(schema_json(schema), indent=2, sort_keys=True) + '\n',
        }
        for path, content in outputs.items():
            if args.check:
                if not path.exists() or path.read_text() != content:
                    raise SystemExit(f'Stale generated artifact: {path}')
            else:
                path.write_text(content)
        print(f'{len(outputs)} generated artifacts {"verified" if args.check else "written"}')


if __name__ == '__main__':
    main()
