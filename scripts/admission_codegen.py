"""Generate nonallocating local admission visitors from the canonical taut IR."""
from taut.ir.model import Scalar, ListOf, MapOf


def emit(schema):
    lines = ['// GENERATED from transport.taut.py; do not edit.',
             'use crate::{codec::{Error, MAX_DATA, MAX_METADATA}, protocol::*};',
             'use crate::budget::Budget;', '',
             'pub(crate) fn allocation_charge(value: &Envelope, limits: &Limits) -> Result<usize, Error> {',
             '    let mut budget = Budget::new(value.stream_id == 0, limits);',
             '    visit_envelope(value, &mut budget, 0)?;',
             '    budget.total_charge()', '}',
             '',
             'pub(crate) fn envelope(value: &Envelope, limits: &Limits) -> Result<(), Error> {',
             '    allocation_charge(value, limits).map(|_| ())', '}', '']

    def visit(t, expr, owner, name, depth):
        access = expr.removeprefix('&')
        number = access if expr.startswith('&') else f'*{expr}'
        if isinstance(t, Scalar):
            if t.kind in ('str', 'bytes'):
                bound = 'MAX_DATA' if (owner, name) == ('Data', 'payload') else 'MAX_METADATA'
                return [f'b.bytes({access}.len(), {bound}, {depth})?;']
            if t.kind == 'int':
                return [f'b.integer({number}, {depth})?;']
            if t.kind == 'bool':
                return [f'let _ = {expr};', f'b.scalar({depth})?;']
            raise ValueError(f'Unsupported admission scalar: {t.kind}')
        if isinstance(t, ListOf):
            inner = visit(t.elem, 'item', owner, name, f'{depth} + 1')
            return [f'b.container({access}.len(), {depth})?;', f'for item in {expr} {{',
                    *['    ' + s for s in inner], '}']
        if isinstance(t, MapOf):
            raise ValueError('Transport admission generator requires explicit Map support')
        if t.name in schema.enums:
            return [f'b.integer({access}.wire(), {depth})?;']
        return [f'visit_{t.name.lower()}({expr}, b, {depth})?;']

    for message in schema.messages.values():
        lines += [f'fn visit_{message.name.lower()}(value: &{message.name}, b: &mut Budget, depth: usize) -> Result<(), Error> {{',
                  f'    b.container({len(message.fields)}, depth)?;']
        if not message.fields:
            lines += ['    let _ = value;']
        for field in message.fields:
            lines += [f'    b.key({field.tag})?;']
            if field.optional:
                lines += [f'    if let Some(item) = &value.{field.name} {{']
                lines += ['        ' + s for s in visit(field.type, 'item', message.name, field.name, 'depth + 1')]
                lines += ['    } else {', '        b.scalar(depth + 1)?;', '    }']
            else:
                lines += ['    ' + s for s in visit(field.type, f'&value.{field.name}', message.name, field.name, 'depth + 1')]
        lines += ['    Ok(())', '}', '']
    return '\n'.join(lines)
