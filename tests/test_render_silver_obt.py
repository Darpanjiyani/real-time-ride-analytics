from tools.render_silver_obt import OUTPUT, render


def test_committed_sql_is_up_to_date():
    assert OUTPUT.read_text() == render(), "run: python tools/render_silver_obt.py"


def test_sql_shape():
    sql = render()
    assert "CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_obt" in sql
    assert "WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES" in sql
    assert sql.count("LEFT JOIN") == 6
    assert ",\nFROM" not in sql  # no trailing comma before FROM
