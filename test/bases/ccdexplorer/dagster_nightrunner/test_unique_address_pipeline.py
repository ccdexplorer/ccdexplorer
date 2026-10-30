"""The unique-address aggregation has to be answerable from index keys.

It reads three fields -- block_height for the range, effect_type for the one
exclusion, impacted_address_canonical for the length and the set -- and
returns two numbers. There is no reason for it to touch a document, and on a
collection of 191 million it cannot afford to: on 2026-10-30 at 00:14 UTC the
monthly run examined 1,417,361 documents in 10.5 seconds, mongod stopped
answering replication heartbeats, and ccd-100 lost the primary and rolled back
eleven seconds of writes.

A compound index on (block_height, effect_type, impacted_address_canonical)
covers all three. The $project has to drop _id for that to hold, because _id
is included by default and is not in the index -- which would pull every
matched document back in and undo the whole thing. These tests are mostly
there to keep that `"_id": 0` from being tidied away by someone who reads it
as redundant.
"""

from ccdexplorer.dagster_nightrunner.nightrunner.update_unique_addresses import (
    unique_address_pipeline,
)

COVERING_INDEX = {"block_height", "effect_type", "impacted_address_canonical"}


def _stage(pipeline, name):
    return next(s[name] for s in pipeline if name in s)


def test_the_range_is_the_one_it_was_given():
    match = _stage(unique_address_pipeline(52_363_293, 53_512_345), "$match")

    assert match["block_height"] == {"$gte": 52_363_293, "$lte": 53_512_345}


def test_the_first_projection_drops_the_document_id():
    """Without this the plan cannot be covered: _id is implicit in a $project
    and is not an index key, so every matched document gets fetched."""
    project = _stage(unique_address_pipeline(0, 1), "$project")

    assert project["_id"] == 0


def test_nothing_downstream_wants_the_document_id():
    """Which is what makes dropping it safe. The $group keys on a category it
    derives, and the final $project builds its own output."""
    pipeline = unique_address_pipeline(0, 1)
    group = _stage(pipeline, "$group")

    assert "$_id" not in repr(group), "the group reads the document's own _id"
    assert pipeline[-1]["$project"]["_id"] == 0


# Names the pipeline computes for itself. They are not stored fields, so an
# index does not need to hold them.
COMPUTED = {"address_length", "unique_addresses", "_id"}


def _collection_fields(node, in_match=False):
    """Every stored field the pipeline reads, by walking it rather than by
    matching text -- $match names its fields as dict keys and everything else
    names them as "$field" strings, and a check that only understands one of
    those two forms passes while missing half the pipeline."""
    found = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key.startswith("$"):
                found |= _collection_fields(value, in_match=in_match or key == "$match")
            else:
                if in_match:
                    found.add(key.split(".")[0])
                found |= _collection_fields(value, in_match=in_match)
    elif isinstance(node, list):
        for item in node:
            found |= _collection_fields(item, in_match=in_match)
    elif isinstance(node, str) and node.startswith("$") and not node.startswith("$$"):
        found.add(node[1:].split(".")[0])
    return found


def test_the_walk_sees_both_ways_a_field_is_named():
    """Guards the check below: it has to pick up a $match key and a "$field"
    reference, or it would pass on a pipeline it never really read."""
    seen = _collection_fields(
        [{"$match": {"a": 1}}, {"$project": {"b": "$c"}}, {"$group": {"_id": "$d.e"}}]
    )

    assert seen == {"a", "c", "d"}


def test_it_reads_no_field_the_index_does_not_hold():
    """A fourth field silently un-covers the plan, and the only symptom is
    that the nightly run goes back to taking ten seconds."""
    fields = _collection_fields(unique_address_pipeline(0, 1)) - COMPUTED

    assert fields == COVERING_INDEX, f"not covered by the index: {fields - COVERING_INDEX}"


def test_the_excluded_effect_type_is_still_excluded():
    """It is the reason the index needs effect_type in it at all."""
    match = _stage(unique_address_pipeline(0, 1), "$match")

    assert match["effect_type"] == {"$ne": "Account Reward"}
