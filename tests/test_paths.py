from lestrade_ingest.paths import accession_from_archive_path, primary_document_from_archive_path


def test_accession_from_archive_path():
    p = "edgar/data/320193/000032019325000042/xslF345X05/wf-form4_1.xml"
    assert accession_from_archive_path(p) == "0000320193-25-000042"


def test_primary_document_from_archive_path():
    p = "edgar/data/320193/000032019325000042/xslF345X05/wf-form4_1.xml"
    assert primary_document_from_archive_path(p) == "xslF345X05/wf-form4_1.xml"
