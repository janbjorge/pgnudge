"""Unit tests for the feed contract dataclasses (Event/Batch/Resync/FeedItem)."""

import dataclasses

import pytest

from pgnudge.core import Batch, Event, FeedItem, Resync


def test_event_count_defaults_to_one() -> None:
    assert Event(payload="public.orders", first_seen=1.0).count == 1


def test_event_is_frozen() -> None:
    event = Event(payload="public.orders", first_seen=1.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        event.count = 2  # type: ignore[misc]


def test_batch_payloads_preserves_order() -> None:
    batch = Batch(
        (
            Event(payload="public.a", first_seen=1.0),
            Event(payload="public.b", first_seen=1.0, count=3),
        )
    )
    assert batch.payloads() == ("public.a", "public.b")


def test_empty_batch_has_no_payloads() -> None:
    assert Batch(()).payloads() == ()


def test_resync_carries_reason() -> None:
    assert Resync("overflow").reason == "overflow"


def test_feeditem_union_membership() -> None:
    assert isinstance(Resync("connected"), FeedItem)
    assert isinstance(Batch(()), FeedItem)
    # an Event is carried inside a Batch; it is not itself a feed item
    assert not isinstance(Event(payload="public.a", first_seen=1.0), FeedItem)


def test_contract_types_are_hashable() -> None:
    # frozen + slots means the items can live in sets / dict keys
    assert len({Resync("connected"), Resync("connected"), Resync("overflow")}) == 2
    assert Event(payload="public.a", first_seen=1.0) == Event(payload="public.a", first_seen=1.0)
