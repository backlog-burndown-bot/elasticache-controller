
	// A secondary joining a Global Datastore inherits engine, engine version, node
	// type, shard count and failover from the datastore, so CreateReplicationGroup
	// rejects any of them sent alongside the membership ID. There is no cross-field
	// CEL knob at this code-generator version, so the guard is a terminal error
	// here. References resolve into Spec.GlobalReplicationGroupID before this hook,
	// so a join requested via the ref already has the concrete ID set while the ref
	// is still present -- test either.
	if err := rejectInheritedFieldsOnDatastoreJoin(desired); err != nil {
		return nil, err
	}
