from kontra.scout.backends.clickhouse_backend import _adapt_expr


def test_adapt_expr_uses_clickhouse_sample_standard_deviation():
    expression = 'STDDEV("age") AS "__std__age"'

    assert _adapt_expr(expression) == 'stddevSamp("age") AS "__std__age"'


def test_adapt_expr_does_not_rewrite_explicit_stddev_variants():
    expression = 'STDDEV_POP("age") AS "__std__age"'

    assert _adapt_expr(expression) == expression
