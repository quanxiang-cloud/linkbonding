package models

import (
	"context"

	"github.com/quanxiang-cloud/linkbonding/pkg/apis/v1alpha1"
)

type NodeRepo interface {
	Insert(ctx context.Context, node *v1alpha1.Node) error

	GetNode(ctx context.Context, start v1alpha1.EndPoint) (*v1alpha1.Node, error)

	Delete(ctx context.Context, node *v1alpha1.Node) error
}
