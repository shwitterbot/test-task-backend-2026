import os
from uuid import uuid4

import pytest
from flask import Flask
from sqlalchemy import create_engine, event, select
from sqlalchemy.schema import CreateSchema, DropSchema

import main
from amocrm import Customer, Lead
from models import AbstractCustomer, db


@pytest.fixture
def postgres():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to a PostgreSQL test database")

    schema = f"sync_test_{uuid4().hex}"
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(CreateSchema(schema))
    app = Flask(__name__)
    app.config.update(
        SQLALCHEMY_DATABASE_URI=url,
        SQLALCHEMY_ENGINE_OPTIONS={"connect_args": {"options": f"-csearch_path={schema}"}},
    )
    db.init_app(app)
    try:
        with app.app_context():
            db.create_all()
            yield
            db.engine.dispose()
    finally:
        with admin.begin() as conn:
            conn.execute(DropSchema(schema, cascade=True))
        admin.dispose()


def stored(id_, login=None, name=None):
    return {"general": {"id": id_, "login": login, "name": name}, "api_value": {}}


def fields(login):
    return {"Логин": {"value": login}} if login else None


def customer(id_, login=None, name=None, status=None, contacts=(), companies=()):
    return Customer(
        id=id_, name=name, next_price=None, next_date=None, responsible_user_id=None,
        status_id=status, periodicity=None, created_by=None, updated_by=None, created_at=None,
        updated_at=None, closest_task_at=None, is_deleted=False, ltv=None, purchases_count=None,
        average_check=None, account_id=None, custom_fields_values=fields(login),
        _embedded={"contacts": list(contacts), "companies": list(companies)},
    )


def lead(id_, login=None, name=None, status=None, contacts=()):
    return Lead(
        id=id_, name=name, status_id=status, custom_fields_values=fields(login),
        _embedded={"contacts": list(contacts)},
    )


def seed(**values):
    group = AbstractCustomer(**values)
    db.session.add(group)
    db.session.flush()
    group_id = group.id
    db.session.commit()
    return group_id


def rows():
    with db.engine.connect() as conn:
        return {
            row.id: row for row in conn.execute(select(AbstractCustomer.__table__)).mappings()
        }


def ids(entities):
    return [entity["general"]["id"] for entity in entities or ()]


def fetch(monkeypatch, customers=(), leads=()):
    monkeypatch.setattr(main, "amocrm_get", lambda cls, **_: iter(customers if cls is Customer else leads))


@pytest.mark.parametrize("batch_size", [1, 500])
def test_merge_keeps_lead_owner_and_seen_related_entities(postgres, monkeypatch, batch_size):
    monkeypatch.setattr(main, "BATCH_SIZE", batch_size)
    target = seed(
        amo_leads=[stored(10, "previous"), stored(11, "previous")],
        amo_contacts=[stored(41, name="УДАЛИТЬ устаревший"), stored(43, name="старый")],
        send_methods=[{"type": "mail", "value": "owner@example.com"}],
    )
    donor = seed(amo_customers=[stored(20, "login"), stored(21, "login")])
    seed(amo_leads=[stored(99)])
    fetch(
        monkeypatch,
        customers=[customer(20, "login", contacts=[{"id": 40}], companies=[{"id": 50}])],
        leads=[lead(10, "login", "новое имя", contacts=[{"id": 41}, {"id": 42}])],
    )

    main.collect_leads_customers()

    result = rows()
    assert donor not in result
    assert list(result) == [target]
    row = result[target]
    assert ids(row.amo_customers) == [20]
    assert ids(row.amo_leads) == [10]
    assert row.amo_leads[0]["general"]["name"] == "новое имя"
    assert set(ids(row.amo_contacts)) == {40, 42}
    assert ids(row.amo_companies) == [50]
    assert row.send_methods == [{"type": "mail", "value": "owner@example.com"}]


def test_customer_matches_id_first_and_ignores_ambiguous_login(postgres, monkeypatch):
    original = seed(amo_customers=[stored(1, "old")])
    unique = seed(amo_leads=[stored(11, "unique"), stored(12, "unique")])
    for lead_id in (13, 14):
        seed(amo_leads=[stored(lead_id, "ambiguous")])
    fetch(monkeypatch, customers=[
        customer(1, "unique", "новое имя"),
        customer(2, "unique"),
        customer(3, "ambiguous"),
        customer(4),
    ])

    main.collect_leads_customers(is_full=False)

    result = rows()
    assert ids(result[original].amo_customers) == [1]
    assert result[original].amo_customers[0]["general"]["name"] == "новое имя"
    assert ids(result[unique].amo_customers) == [2]
    assert sorted(id_ for row in result.values() for id_ in ids(row.amo_customers)) == [1, 2, 3]
    assert len(result) == 5


def test_lead_login_uses_current_batch_memberships(postgres, monkeypatch):
    first = seed(amo_customers=[stored(1, "shared")], amo_leads=[stored(10)])
    second = seed(amo_customers=[stored(2, "shared"), stored(3, "merge")])
    fetch(monkeypatch, leads=[lead(10, "merge"), lead(11, "shared"), lead(12), lead(12, name="final")])

    main.collect_leads_customers(is_full=False)

    result = rows()
    assert second not in result
    assert ids(result[first].amo_customers) == [1, 2, 3]
    assert ids(result[first].amo_leads) == [10, 11]
    standalone = next(row for id_, row in result.items() if id_ != first)
    assert ids(standalone.amo_leads) == [12]
    assert standalone.amo_leads[0]["general"]["name"] == "final"


def test_full_cleanup_removes_marked_and_missing_groups(postgres, monkeypatch):
    keep = seed(amo_customers=[stored(1, "a")], amo_contacts=[stored(3, name="valid")])
    seed(amo_leads=[stored(2)])
    seed()
    fetch(
        monkeypatch,
        customers=[customer(1, "a", contacts=[{"id": 3}])],
        leads=[lead(2, name="Нужно УдАлИтЬ"), lead(4, name="удалить")],
    )

    main.collect_leads_customers()

    result = rows()
    assert list(result) == [keep]
    assert result[keep].amo_contacts[0]["general"]["name"] == "valid"


def test_interrupted_fetch_rolls_back_writes_and_skips_cleanup(postgres, monkeypatch):
    monkeypatch.setattr(main, "BATCH_SIZE", 1)
    seed(amo_leads=[stored(2)])
    seed(amo_customers=[stored(1, "a")])
    before = rows()

    def failing_leads():
        yield lead(2, "a")
        raise RuntimeError("CRM export interrupted")

    fetch(monkeypatch, customers=[customer(3, "new")], leads=failing_leads())
    with pytest.raises(RuntimeError, match="interrupted"):
        main.collect_leads_customers()

    assert rows() == before


def test_incremental_keeps_old_rows_and_skips_excluded_statuses(postgres, monkeypatch):
    original = seed(amo_customers=[stored(1, "a")])
    fetch(
        monkeypatch,
        customers=[customer(2, "excluded", status=111349)],
        leads=[lead(3, status=143), lead(4, name="удалить")],
    )

    main.collect_leads_customers(is_full=False)

    result = rows()
    assert ids(result[original].amo_customers) == [1]
    assert [id_ for row in result.values() for id_ in ids(row.amo_leads)] == [4]


def test_inserts_and_updates_are_batched(postgres, monkeypatch):
    for id_ in range(10):
        seed(amo_leads=[stored(id_)])
    fetch(monkeypatch, leads=[lead(id_, name="updated") for id_ in range(100)])
    statements = []

    def count(_conn, _cursor, stmt, *_):
        statements.append(stmt.lstrip().split()[0].upper())

    event.listen(db.engine, "before_cursor_execute", count)
    main.collect_leads_customers()
    event.remove(db.engine, "before_cursor_execute", count)

    assert statements == ["LOCK", "SELECT", "INSERT", "INSERT", "WITH", "DELETE"]
    assert len(rows()) == 100


def test_full_run_can_be_repeated_and_empty_export_clears_rows(postgres, monkeypatch):
    login = 'логин" || @.general.id == 1 || "\\'
    target = seed(amo_leads=[stored(10, login)])
    fetch(monkeypatch, customers=[customer(1, login)], leads=[lead(10, login)])
    main.collect_leads_customers()
    first = rows()
    assert list(first) == [target]
    assert ids(first[target].amo_customers) == [1]

    main.collect_leads_customers()
    assert rows() == first

    fetch(monkeypatch)
    main.collect_leads_customers()
    assert rows() == {}


@pytest.mark.parametrize("batch_size", [1, 500])
def test_merge_preserves_fresh_entities_from_both_groups(postgres, monkeypatch, batch_size):
    monkeypatch.setattr(main, "BATCH_SIZE", batch_size)
    target = seed(amo_leads=[stored(10)])
    donor = seed(amo_leads=[stored(11)], amo_customers=[stored(1, "shared")])
    fetch(
        monkeypatch,
        customers=[customer(1, "shared")],
        leads=[lead(11, name="fresh donor"), lead(10, "shared", name="fresh target")],
    )

    main.collect_leads_customers()

    result = rows()
    assert list(result) == [target]
    assert donor not in result
    assert ids(result[target].amo_customers) == [1]
    assert {entry["general"]["id"]: entry["general"]["name"]
            for entry in result[target].amo_leads} == {10: "fresh target", 11: "fresh donor"}


def test_lead_does_not_merge_ambiguous_customer_groups(postgres, monkeypatch):
    first = seed(amo_customers=[stored(1, "shared")])
    second = seed(amo_customers=[stored(2, "shared")])
    target = seed(amo_leads=[stored(10)])
    fetch(monkeypatch, leads=[lead(10, "shared"), lead(11, "shared")])

    main.collect_leads_customers(is_full=False)

    result = rows()
    assert ids(result[first].amo_customers) == [1]
    assert ids(result[second].amo_customers) == [2]
    assert ids(result[target].amo_leads) == [10]
    assert len(result) == 4
    assert sorted(id_ for row in result.values() for id_ in ids(row.amo_leads)) == [10, 11]


def test_interrupted_customers_skips_leads_and_cleanup(postgres, monkeypatch):
    monkeypatch.setattr(main, "BATCH_SIZE", 1)
    seed(amo_leads=[stored(10)])
    before = rows()

    def interrupted(cls, **_):
        assert cls is Customer
        yield customer(1, "new")
        raise RuntimeError("customer export interrupted")

    monkeypatch.setattr(main, "amocrm_get", interrupted)
    monkeypatch.setattr(main, "cleanup", lambda *_: pytest.fail("cleanup after interrupted export"))
    with pytest.raises(RuntimeError, match="customer export interrupted"):
        main.collect_leads_customers()

    assert rows() == before


def test_cleanup_database_error_rolls_back_sync(postgres, monkeypatch):
    from sqlalchemy.exc import DBAPIError

    seed(amo_leads=[stored(10)])
    before = rows()
    fetch(monkeypatch, customers=[customer(1, "new")])
    original_cleanup = main.cleanup

    def failing_cleanup(conn, seen):
        original_cleanup(conn, seen)
        conn.exec_driver_sql("SELECT 1 / 0")

    monkeypatch.setattr(main, "cleanup", failing_cleanup)
    with pytest.raises(DBAPIError):
        main.collect_leads_customers()

    assert rows() == before
