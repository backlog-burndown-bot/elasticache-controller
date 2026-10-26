// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package replication_group

import (
	"context"
	"errors"
	"fmt"
	"strings"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	smithy "github.com/aws/smithy-go"

	svcapitypes "github.com/aws-controllers-k8s/elasticache-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/elasticache-controller/pkg/common"
)

const (
	// rolePrimary is the member Role a Global Datastore reports for the primary.
	// Only secondaries can be disassociated.
	rolePrimary = "PRIMARY"
	// codeInvalidGlobalReplicationGroupState is returned while a datastore mutation
	// is already in flight; it is retryable rather than terminal.
	codeInvalidGlobalReplicationGroupState = "InvalidGlobalReplicationGroupState"
)

// datastoreOwnedSpecPaths are the spec fields a Global Datastore owns once a
// replication group is one of its members. ModifyReplicationGroup rejects them
// on a member with InvalidParameterValue, even when the value is unchanged, so
// they can only be changed through the GlobalReplicationGroup.
var datastoreOwnedSpecPaths = []string{
	"Spec.CacheNodeType",
	"Spec.CacheParameterGroupName",
	"Spec.EngineVersion",
	"Spec.AutomaticFailoverEnabled",
}

// isGlobalDatastoreMember reports whether AWS lists the replication group as a
// member of a Global Datastore. It keys off observed status rather than the
// member's role or spec: roles swap on failover, and a primary joins through
// CreateGlobalReplicationGroup without anything in its own spec changing.
func isGlobalDatastoreMember(r *resource) bool {
	info := r.ko.Status.GlobalReplicationGroupInfo
	return info != nil && info.GlobalReplicationGroupID != nil &&
		*info.GlobalReplicationGroupID != ""
}

// reconcileDatastoreOwnedFields stops a member from trying to revert values
// the Global Datastore set. It removes the datastore-owned differences from
// delta and copies the observed node type and parameter group into desired, so
// the returned resource persists them to the CR instead of leaving stale values
// that would be re-applied once the group leaves the datastore.
func reconcileDatastoreOwnedFields(
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) {
	for _, path := range datastoreOwnedSpecPaths {
		if delta.DifferentAt(path) {
			common.RemoveFromDelta(delta, path)
		}
	}
	if latest.ko.Spec.CacheNodeType != nil {
		v := *latest.ko.Spec.CacheNodeType
		desired.ko.Spec.CacheNodeType = &v
	}
	if latest.ko.Spec.CacheParameterGroupName != nil {
		v := *latest.ko.Spec.CacheParameterGroupName
		desired.ko.Spec.CacheParameterGroupName = &v
	}
}

// stripDatastoreOwnedInput removes the datastore-owned fields from a
// ModifyReplicationGroup request for a member. The generated payload sends
// every spec field regardless of the delta, plus AutoMinorVersionUpgrade copied
// from status, so without this any member update is rejected.
func stripDatastoreOwnedInput(
	input *svcsdk.ModifyReplicationGroupInput,
	latest *resource,
) {
	if !isGlobalDatastoreMember(latest) {
		return
	}
	input.CacheNodeType = nil
	input.CacheParameterGroupName = nil
	input.EngineVersion = nil
	input.AutomaticFailoverEnabled = nil
	input.AutoMinorVersionUpgrade = nil
}

// observedGlobalReplicationGroupID returns the datastore ID AWS observed for
// this member, read from status.globalReplicationGroupInfo, or nil when the
// group is not a member.
func observedGlobalReplicationGroupID(ko *svcapitypes.ReplicationGroup) *string {
	info := ko.Status.GlobalReplicationGroupInfo
	if info == nil || info.GlobalReplicationGroupID == nil ||
		*info.GlobalReplicationGroupID == "" {
		return nil
	}
	return info.GlobalReplicationGroupID
}

// observedIfManaged returns the observed value only for a field the user
// actually manages, and nil otherwise. Echoing an observed value into a spec
// field the manifest left unset would make the controller take ownership of it
// and patch it back into the user's manifest; leaving it nil keeps both sides nil
// and, under compare.is_ignored, produces no delta either way.
func observedIfManaged(desired, observed *string) *string {
	if desired == nil {
		return nil
	}
	return observed
}

// requestsDatastoreJoin reports whether the manifest asks to join a Global
// Datastore on create, by either the concrete ID or the reference. A reference
// resolves into Spec.GlobalReplicationGroupID before the create hook runs, so by
// then both are set for a ref-based join; either being present still means a join.
func requestsDatastoreJoin(r *resource) bool {
	return r.ko.Spec.GlobalReplicationGroupID != nil || r.ko.Spec.GlobalReplicationGroupRef != nil
}

// rejectInheritedFieldsOnDatastoreJoin returns a terminal error when a datastore
// join also sets a field the datastore owns. CreateReplicationGroup inherits
// engine, engine version, node type, shard count and failover from the datastore
// and rejects any of them on a member, and it requires at least one replica per
// node group. The error is terminal because none of these can be reconciled away
// -- the manifest has to change.
func rejectInheritedFieldsOnDatastoreJoin(r *resource) error {
	if !requestsDatastoreJoin(r) {
		return nil
	}
	spec := r.ko.Spec
	if spec.Engine != nil || spec.EngineVersion != nil || spec.CacheNodeType != nil ||
		spec.NumNodeGroups != nil || spec.AutomaticFailoverEnabled != nil {
		return ackerr.NewTerminalError(errors.New(
			"spec.globalReplicationGroupID: engine, engineVersion, cacheNodeType, " +
				"numNodeGroups and automaticFailoverEnabled are inherited from the " +
				"datastore and must not be set on a member"))
	}
	if spec.ReplicasPerNodeGroup == nil || *spec.ReplicasPerNodeGroup < 1 {
		return ackerr.NewTerminalError(errors.New(
			"spec.globalReplicationGroupID: replicasPerNodeGroup must be >= 1 for a " +
				"datastore member"))
	}
	return nil
}

// disassociateDatastoreMemberBeforeDelete clears a replication group's Global
// Datastore membership before it can be deleted. It returns a requeue error when
// the delete must be deferred (secondary disassociating, or primary that cannot be
// disassociated at all), a plain error on a real failure, and (nil, nil) when the
// group has already left the datastore and the delete may proceed.
//
// Region and role are read from the datastore member list rather than from this
// group's own read-back, which carries neither: a cross-region secondary must be
// disassociated against its own region, and roles swap on failover so the role is
// re-read at delete time.
func (rm *resourceManager) disassociateDatastoreMemberBeforeDelete(
	ctx context.Context,
	r *resource,
) (requeueErr error, err error) {
	globalID := r.ko.Status.GlobalReplicationGroupInfo.GlobalReplicationGroupID

	descResp, err := rm.sdkapi.DescribeGlobalReplicationGroups(
		ctx,
		&svcsdk.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: globalID,
			ShowMemberInfo:           aws.Bool(true),
		},
	)
	rm.metrics.RecordAPICall("READ_ONE", "DescribeGlobalReplicationGroups", err)
	if err != nil {
		// The datastore being gone means the membership is already gone; let the
		// delete proceed rather than stranding the group.
		if isGlobalReplicationGroupNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(descResp.GlobalReplicationGroups) == 0 {
		return nil, nil
	}

	member, found := findMemberByReplicationGroupID(
		descResp.GlobalReplicationGroups[0].Members,
		r.ko.Spec.ReplicationGroupID,
	)
	if !found {
		// This group is no longer listed as a member; nothing to disassociate.
		return nil, nil
	}

	// A primary cannot be disassociated; the datastore has to go first. Requeue-able
	// rather than terminal, because deleting the datastore clears this on a later pass.
	if member.Role != nil && strings.EqualFold(*member.Role, rolePrimary) {
		return ackrequeue.NeededAfter(
			fmt.Errorf("replication group is the primary of global datastore %s; "+
				"delete the GlobalReplicationGroup first", aws.ToString(globalID)),
			ackrequeue.DefaultRequeueAfterDuration,
		), nil
	}

	if member.ReplicationGroupId == nil || member.ReplicationGroupRegion == nil {
		// A partially reported member is waited out rather than sent as a request
		// AWS would reject.
		return requeueWaitWhileDisassociating, nil
	}

	_, derr := rm.sdkapi.DisassociateGlobalReplicationGroup(
		ctx,
		&svcsdk.DisassociateGlobalReplicationGroupInput{
			GlobalReplicationGroupId: globalID,
			ReplicationGroupId:       member.ReplicationGroupId,
			ReplicationGroupRegion:   member.ReplicationGroupRegion,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "DisassociateGlobalReplicationGroup", derr)
	if derr != nil {
		// Tolerate the races with the GRG's own delete path, which also
		// disassociates secondaries: the datastore already gone, or a
		// disassociation already in flight, both mean "keep waiting".
		if isGlobalReplicationGroupNotFound(derr) ||
			isInvalidGlobalReplicationGroupState(derr) {
			return requeueWaitWhileDisassociating, nil
		}
		return nil, derr
	}
	// Disassociation issued; wait for the next ReadOne to show membership cleared.
	return requeueWaitWhileDisassociating, nil
}

// findMemberByReplicationGroupID returns the datastore member for a given
// replication group ID.
func findMemberByReplicationGroupID(
	members []svcsdktypes.GlobalReplicationGroupMember,
	rgID *string,
) (svcsdktypes.GlobalReplicationGroupMember, bool) {
	if rgID == nil {
		return svcsdktypes.GlobalReplicationGroupMember{}, false
	}
	for _, m := range members {
		if m.ReplicationGroupId != nil && *m.ReplicationGroupId == *rgID {
			return m, true
		}
	}
	return svcsdktypes.GlobalReplicationGroupMember{}, false
}

// isGlobalReplicationGroupNotFound reports whether AWS says the datastore does
// not exist, which on the delete path means the membership is already gone.
func isGlobalReplicationGroupNotFound(err error) bool {
	if err == nil {
		return false
	}
	var notFound *svcsdktypes.GlobalReplicationGroupNotFoundFault
	return errors.As(err, &notFound)
}

// isInvalidGlobalReplicationGroupState reports whether AWS refused a call because
// the datastore is mid-transition, which is retryable. Matched on the typed fault
// and its error code rather than message text, which changes.
func isInvalidGlobalReplicationGroupState(err error) bool {
	if err == nil {
		return false
	}
	var stateFault *svcsdktypes.InvalidGlobalReplicationGroupStateFault
	if errors.As(err, &stateFault) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) &&
		apiErr.ErrorCode() == codeInvalidGlobalReplicationGroupState
}
