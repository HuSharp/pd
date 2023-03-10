// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server provides a set of struct definitions for the resource group, can be imported.
package server

import (
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"go.uber.org/zap"
)

// ResourceGroup is the definition of a resource group, for REST API.
type ResourceGroup struct {
	Name string         `json:"name"`
	Mode rmpb.GroupMode `json:"mode"`
	// RU settings
	RUSettings *RequestUnitSettings `json:"r_u_settings,omitempty"`
}

// RequestUnitSettings is the definition of the RU settings.
type RequestUnitSettings struct {
	RU *GroupTokenBucket `json:"r_u,omitempty"`
}

// NewRequestUnitSettings creates a new RequestUnitSettings with the given token bucket.
func NewRequestUnitSettings(tokenBucket *rmpb.TokenBucket) *RequestUnitSettings {
	return &RequestUnitSettings{
		RU: NewGroupTokenBucket(tokenBucket),
	}
}

func (rg *ResourceGroup) String() string {
	res, err := json.Marshal(rg)
	if err != nil {
		log.Error("marshal resource group failed", zap.Error(err))
		return ""
	}
	return string(res)
}

// Copy copies the resource group.
func (rg *ResourceGroup) Copy() *ResourceGroup {
	return &ResourceGroup{
		Name: rg.Name,
		Mode: rg.Mode,
		RUSettings: &RequestUnitSettings{
			RU: &GroupTokenBucket{
				Settings:              rg.RUSettings.RU.Settings,
				GroupTokenBucketState: *rg.RUSettings.RU.Clone(),
			},
		},
	}
}

// PatchSettings patches the resource group settings.
// Only used to patch the resource group when updating.
// Note: the tokens is the delta value to patch.
func (rg *ResourceGroup) PatchSettings(metaGroup *rmpb.ResourceGroup) error {
	if metaGroup.GetMode() != rg.Mode {
		return errors.New("only support reconfigure in same mode, maybe you should delete and create a new one")
	}
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		settings := metaGroup.GetRUSettings().GetRU()
		if settings == nil {
			return errors.New("invalid resource group settings, RU mode should set RU settings")
		}
		rg.RUSettings.RU.patch(settings)
		log.Info("patch resource group ru settings", zap.String("name", rg.Name), zap.Any("settings", settings))
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
	log.Info("patch resource group settings", zap.String("name", rg.Name), zap.String("settings", rg.String()))
	return nil
}

// FromProtoResourceGroup converts a rmpb.ResourceGroup to a ResourceGroup.
func FromProtoResourceGroup(group *rmpb.ResourceGroup) *ResourceGroup {
	rg := &ResourceGroup{
		Name: group.Name,
		Mode: group.Mode,
	}
	switch group.GetMode() {
	case rmpb.GroupMode_RUMode:
		rg.RUSettings = NewRequestUnitSettings(group.GetRUSettings().GetRU())
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
	return rg
}

// RequestRU requests the RU of the resource group.
func (rg *ResourceGroup) RequestRU(
	now time.Time,
	neededTokens float64,
	targetPeriodMs, clientUniqueID uint64,
) *rmpb.GrantedRUTokenBucket {
	if rg.RUSettings == nil || rg.RUSettings.RU.Settings == nil {
		return nil
	}
	tb, trickleTimeMs := rg.RUSettings.RU.request(now, neededTokens, targetPeriodMs, clientUniqueID)
	return &rmpb.GrantedRUTokenBucket{GrantedTokens: tb, TrickleTimeMs: trickleTimeMs}
}

// IntoProtoResourceGroup converts a ResourceGroup to a rmpb.ResourceGroup.
func (rg *ResourceGroup) IntoProtoResourceGroup() *rmpb.ResourceGroup {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		tokenBucket := &rmpb.TokenBucket{}
		if rg.RUSettings != nil && rg.RUSettings.RU != nil {
			tokenBucket.Settings = rg.RUSettings.RU.Settings
			tokenBucket.Tokens = rg.RUSettings.RU.Tokens
		}
		group := &rmpb.ResourceGroup{
			Name: rg.Name,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: rg.RUSettings.RU.GetTokenBucket(),
			},
		}
		return group
	case rmpb.GroupMode_RawMode: // Raw mode
		panic("no implementation")
	}
	return nil
}

// persistSettings persists the resource group settings.
// TODO: persist the state of the group separately.
func (rg *ResourceGroup) persistSettings(storage endpoint.ResourceGroupStorage) error {
	metaGroup := rg.IntoProtoResourceGroup()
	return storage.SaveResourceGroupSetting(rg.Name, metaGroup)
}

// GroupStates is the tokens set of a resource group.
type GroupStates struct {
	// RU tokens
	RU *GroupTokenBucketState `json:"r_u,omitempty"`
	// raw resource tokens
	CPU     *GroupTokenBucketState `json:"cpu,omitempty"`
	IORead  *GroupTokenBucketState `json:"io_read,omitempty"`
	IOWrite *GroupTokenBucketState `json:"io_write,omitempty"`
}

// GetGroupStates get the token set of ResourceGroup.
func (rg *ResourceGroup) GetGroupStates() *GroupStates {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode: // RU mode
		tokens := &GroupStates{
			RU: rg.RUSettings.RU.GroupTokenBucketState.Clone(),
		}
		return tokens
	case rmpb.GroupMode_RawMode: // Raw mode
		panic("no implementation")
	}
	return nil
}

// SetStatesIntoResourceGroup updates the state of resource group.
func (rg *ResourceGroup) SetStatesIntoResourceGroup(states *GroupStates) {
	switch rg.Mode {
	case rmpb.GroupMode_RUMode:
		if state := states.RU; state != nil {
			rg.RUSettings.RU.setState(states.RU)
			rg.RUSettings.RU.GroupTokenBucketState.tokenSlots = make(map[uint64]*TokenSlot)
		}
	case rmpb.GroupMode_RawMode:
		panic("no implementation")
	}
}

// persistStates persists the resource group tokens.
func (rg *ResourceGroup) persistStates(storage endpoint.ResourceGroupStorage) error {
	states := rg.GetGroupStates()
	return storage.SaveResourceGroupStates(rg.Name, states)
}
