#!/usr/bin/env python3
"""Regenerate transport artifacts with the taut-proto release protocol/generator.json pins.

Ordinary builds need no Python. Generation imports taut only from the taut-proto
release installed in this interpreter's site directories at the pinned version: a
`taut` package or `taut-proto` metadata anywhere else on sys.path or PYTHONPATH is
refused.
"""
import argparse
import importlib.metadata
import json
from pathlib import Path
import site
import subprocess
import sys
import sysconfig
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def verify_formatter(pin):
    actual = subprocess.check_output(['rustfmt', '--version'], text=True).strip()
    if actual != pin['rustfmt']:
        raise SystemExit(f"Expected rustfmt {pin['rustfmt']!r}; got {actual!r}")


def site_directories():
    """This interpreter's site directories, the only places an installed release lives."""
    directories = {Path(sysconfig.get_paths()[key]).resolve() for key in ('purelib', 'platlib')}
    directories.update(Path(path).resolve() for path in site.getsitepackages())
    directories.add(Path(site.getusersitepackages()).resolve())
    return directories


def released_taut(pin):
    """The package directory of the installed taut-proto release the pin names."""
    try:
        distribution = importlib.metadata.distribution('taut-proto')
    except importlib.metadata.PackageNotFoundError:
        raise SystemExit(f"taut-proto {pin['taut-proto']} is not installed") from None
    location = Path(distribution.locate_file('')).resolve()
    if location not in site_directories():
        raise SystemExit(
            f'taut-proto metadata at {location} is not the installed taut-proto release: '
            "it lies outside this interpreter's site directories"
        )
    if distribution.version != pin['taut-proto']:
        raise SystemExit(
            f"taut-proto pin mismatch: expected {pin['taut-proto']}, got {distribution.version}"
        )
    return Path(distribution.locate_file('taut')).resolve()


def verify_taut_modules(package):
    for name, module in list(sys.modules.items()):
        if name == 'taut' or name.startswith('taut.'):
            origin = getattr(module, '__file__', None)
            if not isinstance(origin, str) or package not in Path(origin).resolve().parents:
                raise SystemExit(f'Imported {name} is not the installed taut-proto release: {origin}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    pin = json.loads((ROOT / 'protocol/generator.json').read_text())
    if any(name == 'taut' or name.startswith('taut.') for name in sys.modules):
        raise SystemExit('taut modules are already loaded; use a fresh interpreter')
    package = released_taut(pin)
    import taut  # noqa: F401 -- checked before anything imports from it
    verify_taut_modules(package)
    from admission_codegen import emit as admission
    from taut.gen.scaffold import emit
    from taut.ir.export import schema_json
    from taut.ir.load import load_schema
    from taut.ir.validate import validate_or_raise
    verify_taut_modules(package)
    verify_formatter(pin)
    schema = load_schema(ROOT / 'protocol/transport.taut.py')
    validate_or_raise(schema)
    verify_taut_modules(package)
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
