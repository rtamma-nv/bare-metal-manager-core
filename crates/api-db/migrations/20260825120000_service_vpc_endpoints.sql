-- Per-(attachment, DPU) service-VPC endpoint /127 reservations (DPU
-- block-storage design §6.2). Prefixes are hash-derived, so the exclusion
-- constraint is the collision detector; the composite FK pins each endpoint
-- inside its service VPC's derived /48 and blocks that prefix's hard delete
-- while endpoints exist. No FK to machines/instances: rows are removed
-- in-transaction with the binding that created them.
CREATE TABLE service_vpc_endpoints (
    attachment_id uuid NOT NULL,
    dpu_machine_id varchar(64) NOT NULL,
    extension_service_id uuid NOT NULL REFERENCES extension_services(id),
    instance_id uuid NOT NULL,
    vpc_prefix_id uuid NOT NULL,
    vpc_prefix cidr NOT NULL,
    prefix cidr NOT NULL,
    created timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (attachment_id, dpu_machine_id),
    FOREIGN KEY (vpc_prefix_id, vpc_prefix) REFERENCES network_vpc_prefixes(id, prefix),
    CONSTRAINT service_vpc_endpoint_ipv6_127 CHECK (family(prefix) = 6 AND masklen(prefix) = 127),
    CONSTRAINT service_vpc_endpoint_within_parent CHECK (prefix <<= vpc_prefix),
    CONSTRAINT service_vpc_endpoints_prefix_excl EXCLUDE USING gist (prefix inet_ops WITH &&)
);
CREATE INDEX service_vpc_endpoints_instance_id_idx ON service_vpc_endpoints(instance_id);
CREATE INDEX service_vpc_endpoints_extension_service_id_idx ON service_vpc_endpoints(extension_service_id);
