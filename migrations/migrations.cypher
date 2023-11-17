:begin
CREATE CONSTRAINT nodekey_Unique_type_externalId IF NOT EXISTS FOR (r:Unique)
REQUIRE (r.type, r.external_id) IS NODE KEY;

CREATE CONSTRAINT nodekey_Unique_id IF NOT EXISTS FOR (r:Unique)
REQUIRE r.id IS NODE KEY;

CREATE CONSTRAINT unique_DigitalTwin_id IF NOT EXISTS FOR (r:DigitalTwin)
REQUIRE r.id IS UNIQUE;

CREATE CONSTRAINT unique_Tenant_id IF NOT EXISTS FOR (r:Tenant)
REQUIRE r.id IS UNIQUE;

CREATE CONSTRAINT unique_Source_name IF NOT EXISTS FOR (r:Source)
REQUIRE r.name IS UNIQUE;

CREATE INDEX index_Resource_externalId IF NOT EXISTS FOR (r:Resource) ON r.external_id;

CREATE INDEX index_DigitalTwin_externalId IF NOT EXISTS FOR (r:DigitalTwin) ON r.external_id;

CREATE INDEX index_Property_type_value IF NOT EXISTS FOR (p:Property) ON (p.type, p.value);

CREATE INDEX index_ISSUED_BY_id IF NOT EXISTS FOR ()-[r:ISSUED_BY]-() ON r.id;

:commit
