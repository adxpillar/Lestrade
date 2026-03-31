from decimal import Decimal

from lestrade_ingest.form4_parser import parse_form4_xml

_MINIMAL_XML = b"""<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0306</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2024-01-02</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Test Co</issuerName>
    <issuerTradingSymbol>TEST</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001111111</rptOwnerCik>
      <rptOwnerName>Insider</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common</value></securityTitle>
      <transactionDate>2024-01-01</transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionPricePerShare><value>2.5</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership>D</directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


def test_parse_minimal_form4():
    p = parse_form4_xml(_MINIMAL_XML)
    assert p.document_type == "4"
    assert not p.is_amendment
    assert p.issuer_cik == "0000320193"
    assert p.insider_cik == "0001111111"
    assert len(p.transactions) == 1
    tx = p.transactions[0]
    assert tx.transaction_index == 0
    assert tx.transaction_category == "non_derivative"
    assert tx.transaction_code == "P"
    assert tx.shares == Decimal("10")
    assert tx.price_per_share == Decimal("2.5")
    assert tx.total_value == Decimal("25")
    assert tx.acquired_disposed_code == "A"
    assert tx.direct_indirect == "D"
