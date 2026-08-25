-- An extension service may be bound to a vendor-owned "service VPC" that
-- consumer DPUs realize as shadow VRFs (DPU block-storage design). Nullable:
-- only network-facing services set it. No index: the column is queried only
-- on rare admin paths (service/VPC deletion) against a small table.
ALTER TABLE extension_services ADD COLUMN service_vpc_id uuid REFERENCES vpcs(id);
