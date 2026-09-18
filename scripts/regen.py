#!/usr/bin/env python3
"""Regenerate transport artifacts using taut-proto==0.9.1; ordinary builds need no Python."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

import taut
from admission_codegen import emit as admission
from taut.gen.scaffold import emit
from taut.ir.export import schema_json
from taut.ir.load import load_schema
from taut.ir.validate import validate_or_raise

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    pin = json.loads((ROOT / 'protocol/generator.json').read_text())
    if taut.__version__ != pin['taut-proto']:
        raise SystemExit(f"Expected taut-proto=={pin['taut-proto']}; got {taut.__version__}")
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
