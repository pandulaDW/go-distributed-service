package auth

import (
	"fmt"
	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// New creates a new Authorizer
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{enforcer}
}

// Authorize returns whether the given subject is permitted to run the given action on the
// given object based on the model and policy configured
func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf("%s not permitted to %s to %s", subject, action, object)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
