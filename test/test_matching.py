from runtools.runcore.job import InstanceID, JobInstanceMetadata
from runtools.runcore.matching import criteria, MetadataCriterion, TerminationCriterion
from runtools.runcore.run import TerminationStatus, TerminationInfo, Outcome

from runtools.runcore.util import utc_now, DateTimeRange


def _metadata(*tags):
    return JobInstanceMetadata(InstanceID('j', 'r', 1), tags=tags)


def test_metadata_criterion_tags_matches_when_all_present():
    c = MetadataCriterion(tags=('a', 'b'))
    assert c(_metadata('a', 'b', 'c'))
    assert c(_metadata('a', 'b'))


def test_metadata_criterion_tags_rejects_when_any_missing():
    c = MetadataCriterion(tags=('a', 'b'))
    assert not c(_metadata('a'))
    assert not c(_metadata('b', 'c'))
    assert not c(_metadata())


def test_metadata_criterion_tags_always_and_filter_under_match_any_field():
    """Tags are always an AND filter — never part of the match_any_field OR group.

    A run that matches via id but lacks the required tag must NOT pass.
    """
    from runtools.runcore.util import MatchingStrategy
    c = MetadataCriterion(job_id='import', run_id='import',
                          match_any_field=True, strategy=MatchingStrategy.PARTIAL,
                          tags=('prod',))
    md_id_only = JobInstanceMetadata(InstanceID('import_catalog', 'r', 1), tags=('dev',))
    md_id_and_tag = JobInstanceMetadata(InstanceID('import_catalog', 'r', 1), tags=('prod',))
    assert not c(md_id_only)   # id matches but missing 'prod' tag → fail
    assert c(md_id_and_tag)    # id + tag both match


def test_metadata_criterion_tags_exact_required():
    """Default EXACT strategy requires the tag to be present verbatim."""
    c = MetadataCriterion(job_id='j', tags=('prod',))
    md_match = JobInstanceMetadata(InstanceID('j', 'r', 1), tags=('prod',))
    md_substr = JobInstanceMetadata(InstanceID('j', 'r', 1), tags=('production',))
    assert c(md_match)
    assert not c(md_substr)


def test_metadata_criterion_partial_and_mode_substring_across_axes():
    """PARTIAL strategy in AND mode: id axes + tags all substring-match."""
    from runtools.runcore.util import MatchingStrategy
    c = MetadataCriterion(job_id='alph', run_id='alph', strategy=MatchingStrategy.PARTIAL,
                          tags=('night',))
    md = JobInstanceMetadata(InstanceID('alpha', 'alpha-1', 1), tags=('nightly',))
    assert c(md)


def test_metadata_criterion_tags_normalized_on_construction():
    """Raw user input is normalized at construction (lowercase, # stripped)."""
    c = MetadataCriterion(tags=('#Prod', '#PROD', 'nightly'))
    assert c.tags == ('prod', 'nightly')  # deduped after normalization


def test_metadata_criterion_tags_serialize_round_trip():
    c = MetadataCriterion(tags=('a', 'b'))
    restored = MetadataCriterion.deserialize(c.serialize())
    assert restored.tags == ('a', 'b')


def test_metadata_criterion_no_tags_serialize_omits_keys():
    c = MetadataCriterion(job_id='j')
    d = c.serialize()
    assert 'tags' not in d


def test_parse_bare_token_is_identity_only():
    """parse() returns an identity-only criterion. Tag-axis search is composed separately."""
    c = MetadataCriterion.parse('Shopping')
    assert c.job_id == 'Shopping'
    assert c.run_id == 'Shopping'
    assert c.tags == ()
    assert c.match_any_field


def test_parse_at_path_no_tags():
    """parse('job@run') doesn't set tags — exact identity, not search."""
    c = MetadataCriterion.parse('myjob@myrun')
    assert c.tags == ()
    assert not c.match_any_field


def test_jobrun_criteria_parse_composes_id_and_tag_criteria_for_bare_token():
    """JobRunCriteria.parse('shop') builds two OR'd criteria: id-only + tag-only."""
    from runtools.runcore.util import MatchingStrategy
    from runtools.runcore.matching import JobRunCriteria
    jrc = JobRunCriteria.parse('shop', MatchingStrategy.PARTIAL)
    assert len(jrc.metadata_criteria) == 2
    id_c, tag_c = jrc.metadata_criteria
    assert id_c.job_id == 'shop' and id_c.run_id == 'shop' and id_c.tags == ()
    assert tag_c.job_id == '' and tag_c.run_id == '' and tag_c.tags == ('shop',)


def test_jobrun_criteria_parse_include_tag_false_is_identity_only():
    """include_tag=False suppresses the tag-axis composition — one id-only criterion."""
    from runtools.runcore.matching import JobRunCriteria
    jrc = JobRunCriteria.parse('shop', include_tag=False)
    assert len(jrc.metadata_criteria) == 1
    c = jrc.metadata_criteria[0]
    assert c.tags == ()


def test_builder_patterns_or_all_include_tag_false_keeps_tags_clean():
    """When include_tag=False, explicit tags() are the sole tag filter."""
    built = (criteria()
             .patterns_or_all(['alpha'], include_tag=False)
             .tags('prod')
             .build())
    assert len(built.metadata_criteria) == 1
    c = built.metadata_criteria[0]
    assert c.job_id == 'alpha'
    assert c.tags == ('prod',)   # 'alpha' did NOT seed into tags


def test_post_init_switches_always_true_to_exact_when_tags_present():
    """ALWAYS_TRUE applied to tag filter is nonsensical — switched to EXACT."""
    from runtools.runcore.util import MatchingStrategy
    c = MetadataCriterion(strategy=MatchingStrategy.ALWAYS_TRUE, tags=('prod',))
    assert c.strategy == MatchingStrategy.EXACT
    # ALWAYS_TRUE without tags is preserved (all_match behavior).
    c2 = MetadataCriterion.all_match()
    assert c2.strategy == MatchingStrategy.ALWAYS_TRUE


def test_builder_tags_folds_into_pattern_criteria():
    """tags() accumulates into every metadata criterion at build()."""
    # include_tag=True default → pattern() adds id-only + tag-only criteria.
    built = criteria().pattern('alpha').tags('prod').build()
    assert len(built.metadata_criteria) == 2
    id_c, tag_c = built.metadata_criteria
    assert id_c.job_id == 'alpha' and id_c.tags == ('prod',)
    assert tag_c.tags == ('alpha', 'prod')


def test_builder_tags_without_pattern_creates_exact_tag_criterion():
    """No patterns + tags filter → criterion with EXACT strategy carrying the tags."""
    from runtools.runcore.util import MatchingStrategy
    built = criteria().tags('prod').build()
    assert len(built.metadata_criteria) == 1
    c = built.metadata_criteria[0]
    assert c.strategy == MatchingStrategy.EXACT
    assert c.tags == ('prod',)
    assert c.job_id == '' and c.run_id == ''  # id_match passes via empty-pattern shortcut


def test_builder_tags_folds_into_every_pattern_criterion():
    """tags() folds into all criteria added by pattern() (id-only AND tag-only ones)."""
    built = (criteria()
             .pattern('a', include_tag=False)
             .pattern('b', include_tag=False)
             .tags('prod')
             .build())
    assert len(built.metadata_criteria) == 2
    for c in built.metadata_criteria:
        assert c.tags == ('prod',)


def test_builder_repeated_tags_accumulates():
    built = criteria().tags('a').tags('b', 'c').build()
    c = built.metadata_criteria[0]
    assert c.tags == ('a', 'b', 'c')


def test_interval_utc_conversion():
    c = criteria().created(*DateTimeRange.parse_to_utc(from_val='2023-11-10T09:00+02:00', to_val=None)).build()
    assert c.lifecycle_criteria[-1].created.since.hour == 7


def test_termination_criteria_by_outcome():
    matching = TerminationCriterion(outcome=Outcome.REJECTED)
    not_matching = TerminationCriterion(outcome=Outcome.ABORTED)

    tested_info = TerminationInfo(TerminationStatus.OVERLAP, utc_now())
    assert matching(tested_info)
    assert not not_matching(tested_info)


def test_termination_criteria_by_success():
    success_criterion = TerminationCriterion(success=True)
    nonsuccess_criterion = TerminationCriterion(success=False)

    completed_info = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
    failed_info = TerminationInfo(TerminationStatus.FAILED, utc_now())
    aborted_info = TerminationInfo(TerminationStatus.STOPPED, utc_now())

    assert success_criterion(completed_info)
    assert not success_criterion(failed_info)
    assert not success_criterion(aborted_info)

    assert not nonsuccess_criterion(completed_info)
    assert nonsuccess_criterion(failed_info)
    assert nonsuccess_criterion(aborted_info)
