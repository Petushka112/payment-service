from app.models import OutboxEvent, Payment


def test_enum_columns_store_values_not_member_names():
    assert Payment.__table__.c.status.type.enums == ["pending", "succeeded", "failed"]
    assert Payment.__table__.c.currency.type.enums == ["RUB", "USD", "EUR"]


def test_outbox_has_no_foreign_keys():
    assert not OutboxEvent.__table__.foreign_keys
