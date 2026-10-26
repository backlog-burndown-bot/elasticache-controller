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
	"errors"
	"testing"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	smithy "github.com/aws/smithy-go"

	svcapitypes "github.com/aws-controllers-k8s/elasticache-controller/apis/v1alpha1"
)

// isTerminal reports whether err is (or wraps) an ACK TerminalError.
func isTerminal(err error) bool {
	var te *ackerr.TerminalError
	return errors.As(err, &te)
}

func newMemberTestRG(grgID *string) *resource {
	ko := &svcapitypes.ReplicationGroup{}
	if grgID != nil {
		ko.Status.GlobalReplicationGroupInfo = &svcapitypes.GlobalReplicationGroupInfo{
			GlobalReplicationGroupID:         grgID,
			GlobalReplicationGroupMemberRole: aws.String("PRIMARY"),
		}
	}
	return &resource{ko: ko}
}

func TestIsGlobalDatastoreMember(t *testing.T) {
	cases := map[string]struct {
		id   *string
		want bool
	}{
		"no info":  {nil, false},
		"empty id": {aws.String(""), false},
		"member":   {aws.String("sgaui-grg"), true},
	}
	for name, tc := range cases {
		if got := isGlobalDatastoreMember(newMemberTestRG(tc.id)); got != tc.want {
			t.Errorf("%s: got %t, want %t", name, got, tc.want)
		}
	}
	// Status present with no ID at all.
	r := newMemberTestRG(nil)
	r.ko.Status.GlobalReplicationGroupInfo = &svcapitypes.GlobalReplicationGroupInfo{}
	if isGlobalDatastoreMember(r) {
		t.Error("info without an ID: got member")
	}
}

func TestReconcileDatastoreOwnedFields(t *testing.T) {
	desired := newMemberTestRG(aws.String("sgaui-grg"))
	desired.ko.Spec.CacheNodeType = aws.String("cache.r6g.large")
	desired.ko.Spec.CacheParameterGroupName = aws.String("default.redis7")
	desired.ko.Spec.Description = aws.String("new")

	latest := newMemberTestRG(aws.String("sgaui-grg"))
	latest.ko.Spec.CacheNodeType = aws.String("cache.r6g.xlarge")
	latest.ko.Spec.CacheParameterGroupName = aws.String("global-datastore-redis7")
	latest.ko.Spec.Description = aws.String("old")

	delta := ackcompare.NewDelta()
	delta.Add("Spec.CacheNodeType", desired.ko.Spec.CacheNodeType, latest.ko.Spec.CacheNodeType)
	delta.Add("Spec.CacheParameterGroupName", desired.ko.Spec.CacheParameterGroupName, latest.ko.Spec.CacheParameterGroupName)
	delta.Add("Spec.EngineVersion", aws.String("7.0"), aws.String("7.1"))
	delta.Add("Spec.AutomaticFailoverEnabled", aws.Bool(false), aws.String("enabled"))
	delta.Add("Spec.Description", desired.ko.Spec.Description, latest.ko.Spec.Description)

	reconcileDatastoreOwnedFields(desired, latest, delta)

	for _, p := range datastoreOwnedSpecPaths {
		if delta.DifferentAt(p) {
			t.Errorf("%s still in delta", p)
		}
	}
	if !delta.DifferentAt("Spec.Description") {
		t.Error("Spec.Description was removed; only datastore-owned paths may be")
	}
	if got := *desired.ko.Spec.CacheNodeType; got != "cache.r6g.xlarge" {
		t.Errorf("CacheNodeType = %q, want the observed value", got)
	}
	if got := *desired.ko.Spec.CacheParameterGroupName; got != "global-datastore-redis7" {
		t.Errorf("CacheParameterGroupName = %q, want the observed value", got)
	}
	// The copy must not alias latest.
	*latest.ko.Spec.CacheNodeType = "changed"
	if *desired.ko.Spec.CacheNodeType != "cache.r6g.xlarge" {
		t.Error("desired.CacheNodeType aliases latest")
	}
}

func TestReconcileDatastoreOwnedFields_OnlyOwnedDiffsEmptiesSpecDelta(t *testing.T) {
	desired := newMemberTestRG(aws.String("sgaui-grg"))
	latest := newMemberTestRG(aws.String("sgaui-grg"))
	latest.ko.Spec.CacheParameterGroupName = aws.String("global-datastore-redis7")

	delta := ackcompare.NewDelta()
	delta.Add("Spec.CacheParameterGroupName", aws.String("default.redis7"), latest.ko.Spec.CacheParameterGroupName)

	reconcileDatastoreOwnedFields(desired, latest, delta)
	if delta.DifferentAt("Spec") {
		t.Errorf("delta still differs: %v", delta.Differences)
	}
}

func TestStripDatastoreOwnedInput(t *testing.T) {
	newInput := func() *svcsdk.ModifyReplicationGroupInput {
		return &svcsdk.ModifyReplicationGroupInput{
			CacheNodeType:               aws.String("cache.r6g.large"),
			CacheParameterGroupName:     aws.String("default.redis7"),
			EngineVersion:               aws.String("7.0"),
			AutomaticFailoverEnabled:    aws.Bool(true),
			AutoMinorVersionUpgrade:     aws.Bool(true),
			ReplicationGroupDescription: aws.String("kept"),
		}
	}

	in := newInput()
	stripDatastoreOwnedInput(in, newMemberTestRG(aws.String("sgaui-grg")))
	if in.CacheNodeType != nil || in.CacheParameterGroupName != nil || in.EngineVersion != nil ||
		in.AutomaticFailoverEnabled != nil || in.AutoMinorVersionUpgrade != nil {
		t.Errorf("member input still carries datastore-owned fields: %+v", in)
	}
	if in.ReplicationGroupDescription == nil {
		t.Error("description was stripped")
	}

	in = newInput()
	stripDatastoreOwnedInput(in, newMemberTestRG(nil))
	if in.CacheNodeType == nil || in.AutoMinorVersionUpgrade == nil {
		t.Error("non-member input was modified")
	}
}

// joinRG builds a replication group manifest requesting a datastore join, with
// the minimum valid shape (one replica), that individual cases then perturb.
func joinRG() *resource {
	return &resource{ko: &svcapitypes.ReplicationGroup{
		Spec: svcapitypes.ReplicationGroupSpec{
			GlobalReplicationGroupID: aws.String("sgaui-grg"),
			ReplicasPerNodeGroup:     aws.Int64(1),
		},
	}}
}

func TestRequestsDatastoreJoin(t *testing.T) {
	none := &resource{ko: &svcapitypes.ReplicationGroup{}}
	if requestsDatastoreJoin(none) {
		t.Error("no ID and no ref: should not be a join")
	}
	byID := &resource{ko: &svcapitypes.ReplicationGroup{}}
	byID.ko.Spec.GlobalReplicationGroupID = aws.String("sgaui-grg")
	if !requestsDatastoreJoin(byID) {
		t.Error("concrete ID: should be a join")
	}
	byRef := &resource{ko: &svcapitypes.ReplicationGroup{}}
	byRef.ko.Spec.GlobalReplicationGroupRef = &ackv1alpha1.AWSResourceReferenceWrapper{}
	if !requestsDatastoreJoin(byRef) {
		t.Error("ref only: should be a join")
	}
}

func TestRejectInheritedFieldsOnDatastoreJoin(t *testing.T) {
	// A bare join with one replica is accepted.
	if err := rejectInheritedFieldsOnDatastoreJoin(joinRG()); err != nil {
		t.Errorf("bare join rejected: %v", err)
	}

	// A non-member manifest is never touched, even with inherited fields set.
	nonMember := &resource{ko: &svcapitypes.ReplicationGroup{}}
	nonMember.ko.Spec.Engine = aws.String("redis")
	if err := rejectInheritedFieldsOnDatastoreJoin(nonMember); err != nil {
		t.Errorf("non-member rejected: %v", err)
	}

	inherited := map[string]func(*resource){
		"engine":                   func(r *resource) { r.ko.Spec.Engine = aws.String("redis") },
		"engineVersion":            func(r *resource) { r.ko.Spec.EngineVersion = aws.String("7.1") },
		"cacheNodeType":            func(r *resource) { r.ko.Spec.CacheNodeType = aws.String("cache.r6g.large") },
		"numNodeGroups":            func(r *resource) { r.ko.Spec.NumNodeGroups = aws.Int64(2) },
		"automaticFailoverEnabled": func(r *resource) { r.ko.Spec.AutomaticFailoverEnabled = aws.Bool(true) },
	}
	for name, set := range inherited {
		t.Run("rejects inherited "+name, func(t *testing.T) {
			r := joinRG()
			set(r)
			err := rejectInheritedFieldsOnDatastoreJoin(r)
			if err == nil {
				t.Fatalf("%s set alongside join: expected a terminal error", name)
			}
			if !isTerminal(err) {
				t.Errorf("%s: error is not terminal: %v", name, err)
			}
		})
	}

	t.Run("rejects missing replicasPerNodeGroup", func(t *testing.T) {
		r := joinRG()
		r.ko.Spec.ReplicasPerNodeGroup = nil
		if err := rejectInheritedFieldsOnDatastoreJoin(r); !isTerminal(err) {
			t.Errorf("nil replicasPerNodeGroup: want terminal error, got %v", err)
		}
	})

	t.Run("rejects zero replicasPerNodeGroup", func(t *testing.T) {
		r := joinRG()
		r.ko.Spec.ReplicasPerNodeGroup = aws.Int64(0)
		if err := rejectInheritedFieldsOnDatastoreJoin(r); !isTerminal(err) {
			t.Errorf("zero replicasPerNodeGroup: want terminal error, got %v", err)
		}
	})
}

func TestObservedGlobalReplicationGroupID(t *testing.T) {
	ko := &svcapitypes.ReplicationGroup{}
	if observedGlobalReplicationGroupID(ko) != nil {
		t.Error("no info: want nil")
	}
	ko.Status.GlobalReplicationGroupInfo = &svcapitypes.GlobalReplicationGroupInfo{
		GlobalReplicationGroupID: aws.String(""),
	}
	if observedGlobalReplicationGroupID(ko) != nil {
		t.Error("empty ID: want nil")
	}
	ko.Status.GlobalReplicationGroupInfo.GlobalReplicationGroupID = aws.String("sgaui-grg")
	if got := observedGlobalReplicationGroupID(ko); got == nil || *got != "sgaui-grg" {
		t.Errorf("populated ID: got %v, want sgaui-grg", got)
	}
}

func TestObservedIfManaged(t *testing.T) {
	// Unmanaged (desired nil): never echo the observed value into spec.
	if got := observedIfManaged(nil, aws.String("sgaui-grg")); got != nil {
		t.Errorf("desired nil: want nil, got %v", got)
	}
	// Managed: echo the observed value, even when it diverges from desired (the
	// field is immutable, so the observed datastore ID is the only correct value).
	if got := observedIfManaged(aws.String("declared"), aws.String("sgaui-grg")); got == nil || *got != "sgaui-grg" {
		t.Errorf("desired set: want the observed value, got %v", got)
	}
	// Managed but AWS reports standalone (observed nil): leaves spec nil, so under
	// compare.is_ignored there is no delta.
	if got := observedIfManaged(aws.String("declared"), nil); got != nil {
		t.Errorf("observed nil: want nil, got %v", got)
	}
}

func TestFindMemberByReplicationGroupID(t *testing.T) {
	members := []svcsdktypes.GlobalReplicationGroupMember{
		{ReplicationGroupId: aws.String("rg-a"), ReplicationGroupRegion: aws.String("us-east-1"), Role: aws.String("SECONDARY")},
		{ReplicationGroupId: aws.String("rg-b"), ReplicationGroupRegion: aws.String("us-west-2"), Role: aws.String("PRIMARY")},
	}
	m, found := findMemberByReplicationGroupID(members, aws.String("rg-b"))
	if !found || aws.ToString(m.ReplicationGroupRegion) != "us-west-2" {
		t.Errorf("rg-b: found=%t region=%q, want true/us-west-2", found, aws.ToString(m.ReplicationGroupRegion))
	}
	if _, found := findMemberByReplicationGroupID(members, aws.String("rg-missing")); found {
		t.Error("rg-missing: want not found")
	}
	if _, found := findMemberByReplicationGroupID(members, nil); found {
		t.Error("nil id: want not found")
	}
}

func TestErrorMatchers(t *testing.T) {
	if isGlobalReplicationGroupNotFound(nil) || isInvalidGlobalReplicationGroupState(nil) {
		t.Error("nil error matched")
	}
	notFound := &svcsdktypes.GlobalReplicationGroupNotFoundFault{}
	if !isGlobalReplicationGroupNotFound(notFound) {
		t.Error("GlobalReplicationGroupNotFoundFault not matched")
	}
	stateFault := &svcsdktypes.InvalidGlobalReplicationGroupStateFault{}
	if !isInvalidGlobalReplicationGroupState(stateFault) {
		t.Error("InvalidGlobalReplicationGroupStateFault not matched")
	}
	// The code-only variant AWS also returns, with no Fault suffix on the code.
	byCode := &smithy.GenericAPIError{Code: "InvalidGlobalReplicationGroupState"}
	if !isInvalidGlobalReplicationGroupState(byCode) {
		t.Error("InvalidGlobalReplicationGroupState code not matched")
	}
	// A different fault must not match either matcher.
	other := &smithy.GenericAPIError{Code: "SomethingElse"}
	if isGlobalReplicationGroupNotFound(other) || isInvalidGlobalReplicationGroupState(other) {
		t.Error("unrelated fault matched")
	}
}
