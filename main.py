import logging
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import islice
from typing import Optional

from sqlalchemy import bindparam, delete, func, insert, or_, select, text, true, union
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, insert as pg_insert

from abstract_customer import get_customer_json, get_lead_json
from amocrm import COMPANIES, CONTACTS, Customer, Lead, amocrm_get
from models import AbstractCustomer, db

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
CUSTOMERS = "amo_customers"
LEADS = "amo_leads"
ENTITY_FIELDS = (CUSTOMERS, LEADS, "amo_contacts", "amo_companies")
TABLE = AbstractCustomer.__table__


@dataclass(eq=False)
class Group:
    id: Optional[int] = None
    entities: dict[str, dict[int, dict]] = field(
        default_factory=lambda: {kind: {} for kind in ENTITY_FIELDS}
    )
    seen: dict[str, dict[int, bool]] = field(default_factory=dict)

    def values(self):
        return {kind: list(entities.values()) for kind, entities in self.entities.items()}


class Batch:
    def __init__(self, rows, seen):
        self.by_id = defaultdict(set)
        self.by_login = defaultdict(dict)
        self.dirty = {}
        self.removed = []
        self.seen = seen
        for row in rows:
            group = Group(id=row.id, seen=seen.get(row.id, {}))
            for kind in ENTITY_FIELDS:
                for entity in row[kind] or ():
                    self.put(group, kind, entity)

    def _index(self, group, kind, entity, delta):
        if kind not in (CUSTOMERS, LEADS):
            return
        general = entity["general"]
        matches = self.by_id[kind, general["id"]]
        if delta == 1:
            matches.add(group)
        else:
            matches.remove(group)
        login = general.get("login")
        if login:
            counts = self.by_login[kind, login]
            count = counts.get(group, 0) + delta
            if count:
                counts[group] = count
            else:
                del counts[group]

    def put(self, group, kind, entity):
        entity_id = entity["general"]["id"]
        entries = group.entities[kind]
        if entity_id in entries:
            self._index(group, kind, entries[entity_id], -1)
        entries[entity_id] = entity
        self._index(group, kind, entity, 1)

    def find_by_id(self, kind, entity_id):
        matches = self.by_id.get((kind, entity_id), ())
        # Older databases may already have duplicate CRM IDs. Prefer the oldest row.
        return min(matches, key=lambda group: (group.id is None, group.id or 0), default=None)

    def find_by_login(self, kind, login):
        matches = self.by_login.get((kind, login), ())
        if len(matches) > 1:
            logger.warning("Multiple groups for %s with the same login", kind)
        return next(iter(matches)) if len(matches) == 1 else None

    def merge(self, target, source):
        for kind in ENTITY_FIELDS:
            for entity_id, entity in source.entities[kind].items():
                self._index(source, kind, entity, -1)
                if entity_id not in target.entities[kind] or (
                    entity_id in source.seen.get(kind, {})
                    and entity_id not in target.seen.get(kind, {})
                ):
                    self.put(target, kind, entity)
        for kind, ids in source.seen.items():
            target.seen.setdefault(kind, {}).update(ids)
        self.dirty.pop(source, None)
        if source.id is not None:
            self.removed.append(source.id)
            self.seen.pop(source.id, None)

    def apply(self, kind, crm_entity, is_full):
        login = crm_entity.get_field("Логин")
        group = self.find_by_id(kind, crm_entity.id)
        if kind == CUSTOMERS:
            if group is None:
                group = self.find_by_login(LEADS, login)
            payload = get_customer_json(crm_entity)
        else:
            login_group = self.find_by_login(CUSTOMERS, login) if login else None
            if group is None:
                group = login_group
            elif login_group is not None and login_group is not group:
                self.merge(group, login_group)
            payload = get_lead_json(crm_entity)

        if group is None:
            group = Group()
        self.put(group, kind, payload)
        if is_full:
            group.seen.setdefault(kind, {})[crm_entity.id] = True

        for field_name, related in (
            ("amo_contacts", crm_entity.contacts),
            ("amo_companies", crm_entity.companies),
        ):
            for reference in related:
                entity_id = reference["id"]
                if type(entity_id) is not int:
                    raise ValueError(f"Invalid CRM ID for {field_name}: {entity_id!r}")
                old = group.entities[field_name].get(entity_id, {})
                general = {**old.get("general", {}), "id": entity_id}
                for key in ("name", "phone", "inn"):
                    if key in reference:
                        general[key] = reference[key]
                self.put(group, field_name, {
                    "general": general,
                    "api_value": {**(old.get("api_value") or {}), **reference},
                })
                if is_full:
                    group.seen.setdefault(field_name, {})[entity_id] = True
        self.dirty[group] = None

    def write(self, conn, is_full):
        if self.removed:
            conn.execute(delete(TABLE).where(TABLE.c.id.in_(self.removed)))

        existing = [
            {"id": group.id, **group.values()}
            for group in self.dirty if group.id is not None
        ]
        new_groups = [group for group in self.dirty if group.id is None]
        if new_groups:
            stmt = insert(TABLE).returning(TABLE.c.id, sort_by_parameter_order=True)
            ids = conn.execute(stmt, [group.values() for group in new_groups]).scalars().all()
            for group, group_id in zip(new_groups, ids):
                group.id = group_id

        if existing:
            stmt = pg_insert(TABLE).values(existing)
            conn.execute(stmt.on_conflict_do_update(
                index_elements=[TABLE.c.id],
                set_={
                    **{kind: stmt.excluded[kind] for kind in ENTITY_FIELDS},
                    "updated_at": text("now()"),
                },
                where=or_(*(
                    TABLE.c[kind].is_distinct_from(stmt.excluded[kind]) for kind in ENTITY_FIELDS
                )),
            ))
        if is_full:
            self.seen.update((group.id, group.seen) for group in self.dirty)


def batches(iterable):
    iterator = iter(iterable)
    while batch := list(islice(iterator, BATCH_SIZE)):
        yield batch


def matching_group_ids(column, key, values):
    probes = [[{"general": {key: value}}] for value in values]
    needles = (
        func.unnest(bindparam(f"{key}_matches", probes, type_=ARRAY(JSONB, dimensions=1)))
        .table_valued("needle")
        .render_derived()
    )
    # A parameterized index probe keeps PostgreSQL from comparing every row with every key.
    matches = (
        select(TABLE.c.id)
        .where(column.bool_op("@>")(needles.c.needle))
        .correlate(needles)
        .offset(0)
        .lateral()
    )
    return select(matches.c.id).select_from(needles.join(matches, true()))


def load_batch(conn, kind, entities, seen):
    other_kind = LEADS if kind == CUSTOMERS else CUSTOMERS
    if any(type(entity.id) is not int for entity in entities):
        raise ValueError(f"Invalid CRM ID in batch for {kind}")
    ids = dict.fromkeys(entity.id for entity in entities)
    logins = dict.fromkeys(
        login for entity in entities if (login := entity.get_field("Логин"))
    )
    matches = matching_group_ids(TABLE.c[kind], "id", ids)
    if logins:
        matches = union(matches, matching_group_ids(TABLE.c[other_kind], "login", logins))
    else:
        matches = matches.distinct()
    groups = matches.subquery()
    # Load the matched rows by primary key even when the probe estimates are too high.
    rows = (
        select(TABLE.c.id, *(TABLE.c[field] for field in ENTITY_FIELDS))
        .where(TABLE.c.id == groups.c.id)
        .correlate(groups)
        .offset(0)
        .lateral()
    )
    stmt = select(rows).select_from(groups.join(rows, true()))
    return Batch(conn.execute(stmt).mappings(), seen)


def _filtered_array(kind):
    # Translate only the marker's letters so the comparison also works with C collation.
    return f"""
        COALESCE((
            SELECT jsonb_agg(entry ORDER BY position)
            FROM jsonb_array_elements(NULLIF(customer.{kind}, 'null'::jsonb))
                WITH ORDINALITY AS entries(entry, position)
            WHERE seen.{kind} ? (entry->'general'->>'id')
              AND strpos(
                  translate(COALESCE(entry->'general'->>'name', ''), 'УДАЛИТЬ', 'удалить'),
                  'удалить'
              ) = 0
        ), '[]'::jsonb) AS {kind}
    """


def cleanup(conn, seen):
    columns = ", ".join(f"{kind} jsonb" for kind in ENTITY_FIELDS)
    fields = ", ".join(_filtered_array(kind) for kind in ENTITY_FIELDS)
    assignments = ", ".join(f"{kind} = filtered.{kind}" for kind in ENTITY_FIELDS)
    changes = " OR ".join(f"customer.{kind} IS DISTINCT FROM filtered.{kind}" for kind in ENTITY_FIELDS)
    stmt = text(f"""
        WITH filtered AS MATERIALIZED (
            SELECT customer.id, {fields}
            FROM {TABLE.name} AS customer
            JOIN jsonb_to_recordset(:seen) AS seen(id bigint, {columns}) USING (id)
        )
        UPDATE {TABLE.name} AS customer
        SET {assignments}, updated_at = now()
        FROM filtered
        WHERE customer.id = filtered.id AND ({changes})
    """).bindparams(bindparam("seen", type_=JSONB))

    for batch in batches(seen.items()):
        conn.execute(stmt, {"seen": [{"id": id_, **entities} for id_, entities in batch]})

    conn.execute(text(f"""
        DELETE FROM {TABLE.name} AS customer
        WHERE NOT EXISTS (
            SELECT 1 FROM unnest(:seen_ids) AS seen(id) WHERE seen.id = customer.id
        ) OR (customer.{CUSTOMERS} = '[]'::jsonb AND customer.{LEADS} = '[]'::jsonb)
    """).bindparams(bindparam("seen_ids", type_=ARRAY(TABLE.c.id.type))), {"seen_ids": list(seen)})


def collect_leads_customers(is_full=True):
    """Synchronize CRM groups atomically. The caller supplies the Flask app context."""
    seen = {}
    with db.engine.begin() as conn:
        # Allow readers, but serialize synchronizers and other writes to this table.
        conn.execute(text(f"LOCK TABLE {TABLE.name} IN SHARE ROW EXCLUSIVE MODE"))
        for cls, kind, relations, skipped_status in (
            (Customer, CUSTOMERS, f"{COMPANIES},{CONTACTS}", 111349),
            (Lead, LEADS, CONTACTS, 143),
        ):
            entities = (
                entity for entity in amocrm_get(cls, _with=relations)
                if (is_full or entity.status_id != skipped_status)
                and (kind != CUSTOMERS or entity.get_field("Логин"))
            )
            for entities_batch in batches(entities):
                batch = load_batch(conn, kind, entities_batch, seen)
                for entity in entities_batch:
                    batch.apply(kind, entity, is_full)
                batch.write(conn, is_full)

        if is_full:
            cleanup(conn, seen)
