"""Generate nonallocating local admission visitors from the canonical taut IR."""
from taut.ir.model import Scalar, ListOf, MapOf


def emit(schema):
    lines = ['// GENERATED from transport.taut.py; do not edit.',
             'use crate::{codec::{Error, MAX_DATA, MAX_METADATA}, protocol::*};',
             'use crate::budget::Budget;', '',
             'pub(crate) fn envelope(value: &Envelope, limits: &Limits) -> Result<(), Error> {',
             '    let mut budget = Budget::new(value.stream_id == 0, limits);',
             '    visit_envelope(value, &mut budget)', '}', '']

    def visit(t, expr, owner, name):
        if isinstance(t, Scalar):
            if t.kind in ('str', 'bytes'):
                bound = 'MAX_DATA' if (owner, name) == ('Data', 'payload') else 'MAX_METADATA'
                return [f'b.bytes({expr.lstrip(chr(38))}.len(), {bound})?;']
            return [f'let _ = {expr};', 'b.scalar()?;']
        if isinstance(t, ListOf):
            inner = visit(t.elem, 'item', owner, name)
            return [f'b.container({expr.lstrip(chr(38))}.len())?;', f'for item in {expr} {{', *['    ' + s for s in inner], '}']
        if isinstance(t, MapOf):
            raise ValueError('Transport admission generator requires explicit Map support')
        if t.name in schema.enums:
            return [f'let _ = {expr};', 'b.scalar()?;']
        return [f'visit_{t.name.lower()}({expr}, b)?;']

    for message in schema.messages.values():
        lines += [f'fn visit_{message.name.lower()}(value: &{message.name}, b: &mut Budget) -> Result<(), Error> {{',
                  f'    b.container({len(message.fields)})?;']
        for field in message.fields:
            if field.optional:
                lines += [f'    if let Some(item) = &value.{field.name} {{']
                lines += ['        ' + s for s in visit(field.type, 'item', message.name, field.name)]
                lines += ['    } else {', '        b.scalar()?;', '    }']
            else:
                # Scalar numerics need no reference. Lists/messages are borrowed.
                lines += ['    ' + s for s in visit(field.type, f'&value.{field.name}', message.name, field.name)]
        lines += ['    Ok(())', '}', '']
    return '\n'.join(lines)
