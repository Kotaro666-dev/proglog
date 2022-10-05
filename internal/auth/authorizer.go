package auth

import (
	"fmt"
	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// New / model: Casbinの認可機構を構成するもので、ここではACLを意味している
/// policy: ACLテーブルを含むCSVファイル
func New(model, policy string) *Authorizer {
	enforcer, _ := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	if ok, _ := a.enforcer.Enforce(subject, object, action); !ok {
		message := fmt.Sprintf(
			"%s not permitted to %s to %s", subject, action, object)
		st := status.New(codes.PermissionDenied, message)
		return st.Err()
	}
	return nil
}
