package authority

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

const authorityRandomTokenBytes = 32

func (r *Ready) AllocateGrant(ctx context.Context, ref model.AttemptRef, ordinal model.LaunchOrdinal, options ...ApplyOption) (model.LaunchGrant, DurabilityOutcome, error) {
	if r == nil || r.core == nil {
		return model.LaunchGrant{}, DefinitelyNotCommitted, ErrNotReady
	}
	if err := ref.Validate(); err != nil {
		return model.LaunchGrant{}, DefinitelyNotCommitted, fmt.Errorf("%w: attempt_ref: %v", ErrInvalidRequest, err)
	}
	if err := ordinal.Validate(); err != nil {
		return model.LaunchGrant{}, DefinitelyNotCommitted, fmt.Errorf("%w: launch_ordinal: %v", ErrInvalidRequest, err)
	}
	nonce, err := randomLaunchNonce()
	if err != nil {
		return model.LaunchGrant{}, DefinitelyNotCommitted, err
	}
	grant := model.LaunchGrant{
		Attempt:   ref,
		Ordinal:   ordinal,
		Nonce:     nonce,
		GrantedBy: r.token.boot,
	}
	result, err := r.apply(ctx, ref.JobID, model.CommitGrant{
		Ref:       ref,
		Ordinal:   ordinal,
		Nonce:     model.PermitNonce(nonce),
		GrantedBy: r.token.boot,
	}, options...)
	if err != nil {
		return grant, result.Durability, err
	}
	return grant, result.Durability, nil
}

func randomLaunchNonce() (model.LaunchNonce, error) {
	encoded, err := randomOpaqueToken("grant")
	if err != nil {
		return "", err
	}
	return model.NewLaunchNonce(encoded)
}

func randomOpaqueToken(prefix string) (string, error) {
	var raw [authorityRandomTokenBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "-" + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
